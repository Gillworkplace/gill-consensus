package com.gill.consensus.multipaxos.model;

/**
 * Prepare
 *
 * @author gill
 * @version 2023/08/01
 **/
public class Prepare {

	public int proposalNum;

	public int logIdx;

	@Override
	public String toString() {
		return "Prepare{" + proposalNum + ", " + logIdx + '}';
	}
}
