package com.gill.consensus.raftplus.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * RaftConfig
 *
 * @author gill
 * @version 2023/09/06
 **/
@Getter
@Setter
@ToString
public class RaftConfig {

	private long heartbeatInterval = 100L;

	private long timeoutInterval = 300L;

	private long timeoutRandomFactor = 150;

	private int repairLength = 100;

	private LogConfig logConfig = new LogConfig();

	@Getter
	@Setter
	@ToString
	public static class LogConfig {

		private int loadLen = 30;
	}
}
