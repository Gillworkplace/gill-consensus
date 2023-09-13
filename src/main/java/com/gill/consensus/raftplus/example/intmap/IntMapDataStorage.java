package com.gill.consensus.raftplus.example.intmap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gill.consensus.raftplus.apis.DataStorage;
import com.gill.consensus.raftplus.model.Snapshot;

/**
 * Repository
 *
 * @author gill
 * @version 2023/09/07
 **/
public class IntMapDataStorage implements DataStorage {

	private Map<String, Integer> map = new ConcurrentHashMap<>();

	/**
	 * 设置
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 */
	public void put(String key, int value) {
		map.put(key, value);
	}

	/**
	 * 获取
	 * 
	 * @param key
	 *            key
	 * @return value
	 */
	public Integer get(String key) {
		return map.get(key);
	}

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
