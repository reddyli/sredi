package com.sredi.rdb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RdbUtil {

	public static final String EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
	private static final String MAGIC_STR = "SREDI";

	public static List<Integer> openRdbFile(String dir, String fileName) throws IOException {
		var bytes = Files.readAllBytes(Paths.get(dir + "/" + fileName));
		var result = IntStream.range(0, bytes.length)
			.mapToObj(i -> Byte.toUnsignedInt(bytes[i]))
			.toList();


		//
		// Magic word checking disables for now
//		if (!validateRdb(result)) {
//			throw new IllegalArgumentException("invalid rdb file");
//		}

		return result;
	}

	public static boolean validateRdb(List<Integer> bytes) {
		return MAGIC_STR.equals(convertIntByteListToString(bytes.subList(0, 5)));
	}

	public static List<Integer> convertStringToByIntByteList(String str) {
		var bytes = str.getBytes(Charset.defaultCharset());
		return IntStream.range(0, bytes.length)
			.mapToObj(i -> Byte.toUnsignedInt(bytes[i]))
			.toList();
	}

	public static String convertIntByteListToString(List<Integer> bytes) {
		return bytes.stream()
			.map(Character::toString)
			.collect(Collectors.joining());
	}
}
