package com.gill.consensus.raftplus.mock;

import java.util.List;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.machine.RaftState;
import com.gill.consensus.raftplus.model.LogEntry;

/**
 * MockNode
 *
 * @author gill
 * @version 2023/09/04
 **/
public class MockNode extends Node implements TestMethod {

	public MockNode(int id) {
		super(id);
	}

	@Override
	public boolean isUp() {
		return machine.getState() != RaftState.STRANGER;
	}

	@Override
	public boolean isLeader() {
		return machine.getState() == RaftState.LEADER;
	}

	@Override
	public boolean isFollower() {
		return machine.getState() == RaftState.FOLLOWER;
	}

	@Override
	public List<LogEntry> getLog() {
		return logManager.getLogs(0, Integer.MAX_VALUE);
	}
}
