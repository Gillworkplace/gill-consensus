package com.gill.consensus.raftplus.entity;

import lombok.Getter;
import lombok.ToString;

/**
 * ReplicateSnapshotParam
 *
 * @author gill
 * @version 2023/09/12
 **/
@Getter
@ToString(callSuper = true)
public class ReplicateSnapshotParam extends BaseParam {

	private final int applyIdx;

	private final long applyTerm;

	private final byte[] data;

	public ReplicateSnapshotParam(int nodeId, long term, int applyIdx, long applyTerm, byte[] data) {
		super(nodeId, term);
		this.applyIdx = applyIdx;
		this.applyTerm = applyTerm;
		this.data = data;
	}
}
