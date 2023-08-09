package com.gill.consensus.raft.model;

import java.util.List;

import com.gill.consensus.raft.Node;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * LogEntries
 *
 * @author gill
 * @version 2023/08/03
 **/
@NoArgsConstructor
@AllArgsConstructor
public class LogEntries {

	public Node node;

	public int term;

	public List<LogEntry> logEntries;

	public int prevLogIndex;

	public int prevLogTerm;

	public int committedIndex;

	public LogEntries(Node node, int term) {
		this.node = node;
		this.term = term;
	}

	@Override
	public String toString() {
		return "LogEntries{" + "term=" + term + ", logEntries=" + logEntries + ", prevLogIndex=" + prevLogIndex
				+ ", prevLogTerm=" + prevLogTerm + ", committedIndex=" + committedIndex + '}';
	}
}
