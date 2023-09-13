package com.gill.consensus.raftplus.machine;

import lombok.Builder;
import lombok.Getter;

/**
 * RaftEventParams
 *
 * @author gill
 * @version 2023/09/04
 **/
@Builder
@Getter
public class RaftEventParams {

	public static final RaftEventParams EMPTY = new RaftEventParams(-1, -1);

	private long term;

	private int votedFor;
}
