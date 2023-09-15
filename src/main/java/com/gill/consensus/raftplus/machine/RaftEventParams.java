package com.gill.consensus.raftplus.machine;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.CountDownLatch;

/**
 * RaftEventParams
 *
 * @author gill
 * @version 2023/09/04
 **/
@Setter
@Getter
public class RaftEventParams {

	private long term;

	private boolean sync;

	private CountDownLatch latch = new CountDownLatch(1);

	public RaftEventParams(long term) {
		this.term = term;
	}

	public RaftEventParams(long term, boolean sync) {
		this.term = term;
		this.sync = sync;
	}
}
