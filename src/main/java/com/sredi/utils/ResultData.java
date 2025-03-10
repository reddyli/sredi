package com.sredi.utils;

import java.util.ArrayList;
import java.util.List;

public record ResultData(
	DataType dataType,
	String data
) {
	public static List<ResultData> getSimpleResultData(DataType dataType, String data) {
		return List.of(new ResultData(dataType, data));
	}

	public static List<ResultData> getBulkStringData(String data) {
		var sizeData = new ResultData(DataType.BULK_STRINGS, data == null ? "-1" : String.valueOf(data.length()));
		var strData = new ResultData(DataType.EMPTY_TYPE, data);

		return data == null ? List.of(sizeData) : List.of(sizeData, strData);
	}

	public static List<ResultData> getArrayData(String... args) {
		var result = new ArrayList<ResultData>();

		// array size data
		result.add(new ResultData(DataType.ARRAYS, String.valueOf(args.length)));
		for (var str : args) {
			result.addAll(getBulkStringData(str));
		}

		return result;
	}

	public static String convertToOutputString(List<ResultData> resultDataList) {
		var result = new StringBuilder();

		for (var resultData : resultDataList) {
			result.append(resultData.dataType().getFirstByte());
			result.append(resultData.data());
			if (resultData.dataType.isTrailing()) {
				result.append("\r\n");
			}
		}

		return result.toString();
	}
}
