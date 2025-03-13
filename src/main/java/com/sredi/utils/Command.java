package com.sredi.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum Command {
	// Basic Command
	ECHO(true, false),
	SET(true, true),
	GET(true, false),
	DEL(true, true),
	CONFIG(true, false),
	KEYS(true, false),
	TYPE(true, false),

	// Replication Command
	INFO(true, false),
	PING(true, false),
	REPLCONF(true, false),
	PSYNC(false, false),
	WAIT(true, false);

	private static final Map<String, Command> commandMap = Arrays.stream(Command.values())
		.collect(Collectors.toMap(command -> command.name().toLowerCase(), it -> it));

	public static Command parseCommand(String command) {
		return commandMap.get(command.toLowerCase());
	}

	private final boolean isReadOperation;
	private final boolean isWriteOperation;
}
