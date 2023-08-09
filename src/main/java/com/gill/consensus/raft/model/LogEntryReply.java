package com.gill.consensus.raft.model;

import lombok.ToString;

import java.util.concurrent.CountDownLatch;

/**
 * LogEntryReply
 *
 * @author gill
 * @version 2023/08/04
 **/
@ToString
public class LogEntryReply {

	public LogEntry logEntry;

	public Reply reply;

	public CountDownLatch latch;

	public LogEntryReply(LogEntry logEntry, CountDownLatch latch) {
		this.logEntry = logEntry;
		this.latch = latch;
		this.reply = new Reply();
	}
}
