package com.gill.consensus.paxos.model;

/**
 * Proposal
 *
 * @author gill
 * @version 2023/07/31
 **/
public class Proposal {

	public int proposerId;

	public int proposalNumber;

	public int proposalValue;

	@Override
	public String toString() {
		return "[" + proposerId + "," + proposalNumber + "," + proposalValue + "]";
	}
}
