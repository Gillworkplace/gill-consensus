package com.gill.consensus.multipaxos.model;

/**
 * Success
 *
 * @author gill
 * @version 2023/08/01
 **/
public class Success {

	public int logIdx;

	public int val;

	@Override
	public String toString() {
		return "Success{" + logIdx + ", " + val + '}';
	}
}
