package com.gill.consensus.multipaxos.model;

/**
 * Accept
 *
 * @author gill
 * @version 2023/08/01
 **/
public class Accept {

	public int proposalNum;

	public int logIdx;

	public int proposalVal;

	public int firstUnchosenIndex;

	@Override
	public String toString() {
		return "Accept{" + proposalNum + ", " + logIdx + ", " + proposalVal + ", " + firstUnchosenIndex + '}';
	}
}
