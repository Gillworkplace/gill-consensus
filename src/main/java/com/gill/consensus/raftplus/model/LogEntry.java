package com.gill.consensus.raftplus.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * LogEntity
 *
 * @author gill
 * @version 2023/09/07
 **/
@Getter
@AllArgsConstructor
@ToString
public class LogEntry {

	private int index;

	private long term;

	private String command;
}
