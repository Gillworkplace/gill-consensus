package com.gill.consensus.raftplus;

import javafx.util.Pair;
import lombok.ToString;

/**
 * HeartbeatState
 *
 * @author gill
 * @version 2023/09/06
 **/
@ToString
public class HeartbeatState {

	private long lastHeartbeatTerm;

	private long lastHeartbeatTimestamp;

	public HeartbeatState(long lastHeartbeatTerm, long lastHeartbeatTimestamp) {
		this.lastHeartbeatTerm = lastHeartbeatTerm;
		this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
	}

	/**
	 * set
	 * 
	 * @param lastHeartbeatTerm
	 *            上次心跳收到的任期
	 * @param lastHeartbeatTimestamp
	 *            上次收到心跳的时间
	 */
	public synchronized void set(long lastHeartbeatTerm, long lastHeartbeatTimestamp) {
		if (lastHeartbeatTerm < this.lastHeartbeatTerm) {
			return;
		}
		this.lastHeartbeatTerm = lastHeartbeatTerm;
		this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
	}

	/**
	 * get
	 * 
	 * @return lastHeartbeatTerm, lastHeartbeatTimestamp
	 */
	public synchronized Pair<Long, Long> get() {
		return new Pair<>(this.lastHeartbeatTerm, this.lastHeartbeatTimestamp);
	}
}
