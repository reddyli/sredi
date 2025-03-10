package com.sredi.common;

import java.util.Random;

public class RandomStringUtil {

	private static final Random RANDOM = new Random();

	public static String createRandomString(int length) {
		var leftLimit = 65;
		var rightLimit = 122;

		return RANDOM.ints(leftLimit, rightLimit + 1)
			.filter(i -> (i <= 90 || i >= 97))
			.limit(length)
			.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
			.toString();
	}
}
