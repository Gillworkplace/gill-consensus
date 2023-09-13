package com.gill.consensus.raftplus.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Snapshot
 *
 * @author gill
 * @version 2023/09/12
 **/
@Getter
@AllArgsConstructor
public class Snapshot {

	private long applyTerm;

	private int applyIdx;

	private byte[] data;
}
