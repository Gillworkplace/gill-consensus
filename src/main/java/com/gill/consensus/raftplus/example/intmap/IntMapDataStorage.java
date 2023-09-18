package com.gill.consensus.raftplus.example.intmap;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.gill.consensus.raftplus.apis.VersionDataStorage;
import com.gill.consensus.raftplus.model.Snapshot;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * Repository
 *
 * @author gill
 * @version 2023/09/07
 **/
@Slf4j
public class IntMapDataStorage extends VersionDataStorage {

	private Map<String, Integer> map = new HashMap<>(1024);

	private final IntMapCommandSerializer serializer = new IntMapCommandSerializer();

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
		map = new HashMap<>(1024);
		return 0;
	}

	@Override
	public byte[] getSnapshotData() {
		String mapStr = JSONUtil.toJsonStr(map);
		return mapStr.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public String apply(String command) {
		IntMapCommand cm = serializer.deserialize(command);
		return cm.execute(map, cm);
	}

	@Override
	public void saveSnapshotToFile(Snapshot snapshot) {
		log.debug("ignore saving snapshot to file");
	}

	@Override
	public void saveSnapshot(byte[] data) {
		map = JSONUtil.toBean(new String(data, StandardCharsets.UTF_8), new TypeReference<Map<String, Integer>>() {
		}, true);
	}

	@Override
	public String println() {
		return JSONUtil.toJsonStr(map);
	}
}
