package com.sredi.rdb;

import java.util.ArrayList;
import java.util.List;

import com.sredi.rdb.model.*;

//Official RESP Docs on RDB File https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
public class RdbBuilder {
	private List<Integer> bytes;
	private int pos = 0;

	private static final int DB_SELECTOR = 0xFE;
	private static final int AUXILIARY_START_BIT = 0xFA;
	private static final int RESIZEDB_START_BIT = 0xFB;
	private static final int EXPIRY_TIME_IN_MS_BIT = 0xFC;
	private static final int EXPIRY_TIME_IN_SECOND_BIT = 0xFD;

	public RdbBuilder bytes(List<Integer> bytes) {
		this.bytes = bytes;
		return this;
	}

	public Rdb build() {
		pos = bytes.indexOf(AUXILIARY_START_BIT);
		var auxField = getAuxFields();
		var dbInfo = getDbInfo();

		return new Rdb(auxField, dbInfo);
	}

	private List<RdbDbInfo> getDbInfo() {
		var result = new ArrayList<RdbDbInfo>();

		while (bytes.get(pos) == DB_SELECTOR) {
			pos++;
			var dbNumber = bytes.get(pos);
			if (bytes.get(pos + 1) != RESIZEDB_START_BIT) {
				break;
			}

			pos += 2;
			var hashTableSizeMetadata = getEncodingMetadata(pos);
			var hashTableSize = hashTableSizeMetadata.value();
			var expireHashTableSizeMetadata = getEncodingMetadata(pos + hashTableSizeMetadata.encoding().getSize());
			var expireHashTableSize = expireHashTableSizeMetadata.value();
			pos = pos + hashTableSizeMetadata.encoding().getSize() + expireHashTableSizeMetadata.encoding().getSize();

			var expireHashTablePair = new ArrayList<RdbExpirePair>();
			var hashTablePair = new ArrayList<RdbPair>();

			for (int i = 0; i < hashTableSize; i++) {
				if (bytes.get(pos) == EXPIRY_TIME_IN_MS_BIT || bytes.get(pos) == EXPIRY_TIME_IN_SECOND_BIT) {
					long expireTime = 0L;
					var count = bytes.get(pos) == EXPIRY_TIME_IN_MS_BIT ? 8 : 4;
					pos++;

					for (var idx = pos + count - 1; idx >= pos; idx--) {
						expireTime <<= 8;
						expireTime |= bytes.get(idx);
					}

					if (bytes.get(pos) == EXPIRY_TIME_IN_SECOND_BIT) {
						expireTime *= 1000;
					}

					var keyValuePairAndOffset = getRdbKeyValuePairAndOffset(pos + count);
					pos = keyValuePairAndOffset.offset;
					expireHashTablePair.add(new RdbExpirePair(expireTime, keyValuePairAndOffset.rdbPair.key(), keyValuePairAndOffset.rdbPair.value()));
				} else {
					var keyValuePairAndOffset = getRdbKeyValuePairAndOffset(pos);

					hashTablePair.add(keyValuePairAndOffset.rdbPair);
					pos = keyValuePairAndOffset.offset;
				}
			}

			result.add(new RdbDbInfo(dbNumber, hashTableSize, expireHashTableSize, expireHashTablePair, hashTablePair));
		}

		return result;
	}

	private List<AuxField> getAuxFields() {
		var result = new ArrayList<AuxField>();

		while (bytes.get(pos) == AUXILIARY_START_BIT) {
			var keyAndOffset = extractValueAndNewOffset(pos + 1);
			var key = keyAndOffset.data();
			pos = keyAndOffset.offset();

			var valueAndOffset = extractValueAndNewOffset(pos);
			var value = valueAndOffset.data();
			pos = valueAndOffset.offset();

			result.add(new AuxField(key, value));
		}

		return result;
	}

	private RdbKeyValuePairAndOffset getRdbKeyValuePairAndOffset(int pos) {
		pos++;
		var keyDataAndNewOffset = extractValueAndNewOffset(pos);
		var key = keyDataAndNewOffset.data();
		pos = keyDataAndNewOffset.offset();

		var valueDataAndNewOffset = extractValueAndNewOffset(pos);
		var value = valueDataAndNewOffset.data();
		pos = valueDataAndNewOffset.offset();

		return new RdbKeyValuePairAndOffset(new RdbPair(key, value), pos);
	}

	private RdbDataAndOffset extractValueAndNewOffset(int pos) {
		var encodingMetadata = getEncodingMetadata(pos);
		if (encodingMetadata.encoding().isLength()) {
			var startPos = pos + encodingMetadata.encoding().getSize();
			var data = RdbUtil.convertIntByteListToString(bytes.subList(startPos, startPos + encodingMetadata.value()));
			return new RdbDataAndOffset(data, startPos + encodingMetadata.value());
		} else {
			return new RdbDataAndOffset(String.valueOf(encodingMetadata.value()), pos + encodingMetadata.encoding().getSize());
		}
	}

	private RdbEncodingMetadata getIntegerFromSpecialFormat(int pos) {
		int formatByte = bytes.get(pos);
		int flag = formatByte & 63; // 00111111 (2)

		return switch (flag) {
			case 0 -> new RdbEncodingMetadata(RdbEncoding.INTEGER_SHORT, bytes.get(pos + 1));
			case 1 -> new RdbEncodingMetadata(RdbEncoding.INTEGER_MIDDLE, (bytes.get(pos + 1) << 8) | bytes.get(pos + 2));
			case 2 -> new RdbEncodingMetadata(RdbEncoding.INTEGER_LONG, (bytes.get(pos + 1) << 24) | (bytes.get(pos + 2) << 16) | (bytes.get(pos + 3) << 8) | bytes.get(pos + 4));
			default -> throw new UnsupportedOperationException("unsupported"); // Unsupported Compressed String
		};
	}

	private RdbEncodingMetadata getEncodingMetadata(int pos) {
		int sizeByte = bytes.get(pos);

		int flag = sizeByte >> 6;
		return switch (flag) {
			case 0 -> new RdbEncodingMetadata(RdbEncoding.LENGTH_SHORT, sizeByte & 63); // 00111111 (2)
			case 1 -> new RdbEncodingMetadata(RdbEncoding.LENGTH_MIDDLE, (sizeByte << 8) | bytes.get(pos + 1));
			case 2 -> new RdbEncodingMetadata(RdbEncoding.LENGTH_LONG, (bytes.get(pos + 1) << 24) | (bytes.get(pos + 2) << 16) | (bytes.get(pos + 3) << 8) | bytes.get(pos + 4));
			case 3 -> getIntegerFromSpecialFormat(pos);
			default -> null;
		};
	}

	record RdbDataAndOffset(String data, int offset) {
	}

	record RdbEncodingMetadata(RdbEncoding encoding, int value) {
	}

	record RdbKeyValuePairAndOffset(RdbPair rdbPair, int offset) {

	}
}
