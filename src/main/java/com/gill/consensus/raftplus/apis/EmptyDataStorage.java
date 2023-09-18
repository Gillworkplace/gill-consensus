package com.gill.consensus.raftplus.apis;

import com.gill.consensus.raftplus.model.Snapshot;

/**
 * EmptyRepository
 *
 * @author gill
 * @version 2023/09/07
 **/
public class EmptyDataStorage extends VersionDataStorage {

	@Override
	public int getApplyIdx() {
		return 0;
	}

	@Override
	public byte[] getSnapshotData() {
		return new byte[0];
	}

	@Override
	public int loadSnapshot() {
		return 0;
	}
	

	@Override
	public void saveSnapshotToFile(Snapshot snapshot) {

	}

	@Override
	public void saveSnapshot(byte[] data) {

	}

	@Override
	public String apply(String command) {
		return "";
	}

	@Override
	public String println() {
		return "";
	}
}
