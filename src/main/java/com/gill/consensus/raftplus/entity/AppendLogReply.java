package com.gill.consensus.raftplus.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * AppendLogReply
 *
 * @author gill
 * @version 2023/09/11
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class AppendLogReply extends Reply {

	private boolean syncSnapshot = false;

	private int compareIdx = -1;

	public AppendLogReply(boolean success, long term) {
		super(success, term);
	}

	public AppendLogReply(boolean success, long term, boolean syncSnapshot) {
		super(success, term);
		this.syncSnapshot = syncSnapshot;
	}

	public AppendLogReply(boolean success, long term, boolean syncSnapshot, int compareIdx) {
		super(success, term);
		this.syncSnapshot = syncSnapshot;
		this.compareIdx = compareIdx;
	}
}
