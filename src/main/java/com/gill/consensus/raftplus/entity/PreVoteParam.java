package com.gill.consensus.raftplus.entity;

import lombok.Getter;
import lombok.ToString;

/**
 * PreVoteParam
 *
 * @author gill
 * @version 2023/08/18
 **/
@Getter
@ToString(callSuper = true)
public class PreVoteParam extends BaseParam {

	private final long lastLogTerm;

	private final int lastLogIdx;

	public PreVoteParam(int nodeId, long term, long lastLogTerm, int lastLogIdx) {
		super(nodeId, term);
		this.lastLogTerm = lastLogTerm;
		this.lastLogIdx = lastLogIdx;
	}
}
