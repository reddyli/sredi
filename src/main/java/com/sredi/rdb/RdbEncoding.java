package com.sredi.rdb;

public enum RdbEncoding {
	LENGTH_SHORT(1, true),
	LENGTH_MIDDLE(2, true),
	LENGTH_LONG(5, true),
	INTEGER_SHORT(2, false),
	INTEGER_MIDDLE(3, false),
	INTEGER_LONG(5, false);

	private final int size;
	private final boolean isLength;

	RdbEncoding(int size, boolean isLength) {
		this.size = size;
		this.isLength = isLength;
	}

	public int getSize() {
		return size;
	}

	public boolean isLength() {
		return isLength;
	}
}
