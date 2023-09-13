package com.gill.consensus.raftplus.config;

import lombok.Getter;
import lombok.Setter;

/**
 * RaftConfig
 *
 * @author gill
 * @version 2023/09/06
 **/
@Getter
@Setter
public class RaftConfig {

	private long heartbeatInterval = 100L;

	private long timeoutInterval = 300L;

	private long timeoutRandomFactor = 150;

	private int repairLength = 100;

	private LogConfig logConfig = new LogConfig();

	@Getter
	@Setter
	public static class LogConfig {

		private int loadLen = 30;
	}
}
