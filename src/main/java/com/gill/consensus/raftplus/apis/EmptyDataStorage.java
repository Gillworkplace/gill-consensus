package com.gill.consensus.raftplus.apis;

import com.gill.consensus.raftplus.model.Snapshot;

/**
 * EmptyRepository
 *
 * @author gill
 * @version 2023/09/07
 **/
public class EmptyDataStorage implements DataStorage {

	@Override
	public int getApplyIdx() {
		return 0;
	}

	@Override
	public int loadSnapshot() {
		return 0;
	}

	@Override
	public Snapshot getSnapshot() {
		return new Snapshot(0, 0, new byte[0]);
	}

	@Override
	public void saveSnapshot() {

	}

	@Override
	public void saveSnapshot(long term, int applyIdx, byte[] data) {

	}

	@Override
	public String apply(String command) {
		return "";
	}

	@Override
	public String apply(int logIdx, String command) {
		return "";
	}

	@Override
	public String println() {
		return "";
	}
}
