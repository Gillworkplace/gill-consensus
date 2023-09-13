package com.gill.consensus.raftplus;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * PersistentProperties
 *
 * @author gill
 * @version 2023/09/11
 **/
@Getter
@Setter
@ToString
public class PersistentProperties {

	private long term;

	private Integer votedFor;

	public void set(PersistentProperties properties) {
		term = properties.getTerm();
		votedFor = properties.getVotedFor();
	}
}
