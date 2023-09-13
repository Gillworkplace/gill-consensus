package com.gill.consensus.raftplus.example.intmap;

import com.gill.consensus.raftplus.apis.CommandSerializer;

import cn.hutool.json.JSONUtil;

/**
 * IntMapCommandSerializer
 *
 * @author gill
 * @version 2023/09/07
 **/
public class IntMapCommandSerializer implements CommandSerializer<IntMapCommand> {

	@Override
	public String serialize(IntMapCommand command) {
		return JSONUtil.toJsonStr(command);
	}

	@Override
	public IntMapCommand deserialize(String commandStr) {
		return JSONUtil.toBean(commandStr, IntMapCommand.class);
	}
}
