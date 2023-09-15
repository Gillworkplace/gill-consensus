package com.gill.consensus.raftplus;

import javafx.util.Pair;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * HeartbeatState
 *
 * @author gill
 * @version 2023/09/06
 **/
@Slf4j
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
	 * @param term
	 *            上次心跳收到的任期
	 * @param heartbeatTimestamp
	 *            上次收到心跳的时间
	 */
	public synchronized void set(long term, long heartbeatTimestamp) {
		if (term < this.lastHeartbeatTerm) {
			log.debug("discard heartbeat");
			return;
		}
		this.lastHeartbeatTerm = term;
		this.lastHeartbeatTimestamp = Math.max(heartbeatTimestamp, this.lastHeartbeatTimestamp);
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
