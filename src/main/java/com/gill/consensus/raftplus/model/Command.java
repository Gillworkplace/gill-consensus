package com.gill.consensus.raftplus.model;

import lombok.Builder;
import lombok.ToString;

/**
 * Command
 *
 * @author gill
 * @version 2023/09/07
 **/
@Builder
@ToString
public class Command {

	public enum Type {

		/**
		 * 不会影响状态机状态的操作
		 */
		SELECT,

		/**
		 * 会印象状态机状态的操作
		 */
		UPDATE
	}

	private Type type;

	private String command;
}
