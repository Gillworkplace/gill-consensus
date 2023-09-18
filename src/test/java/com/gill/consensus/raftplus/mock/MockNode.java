package com.gill.consensus.raftplus.mock;

import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.machine.RaftMachine;
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

	public RaftMachine getRaftMachine() {
		return (RaftMachine) ReflectionTestUtils.getField(this, "machine");
	}

	@Override
	public boolean isUp() {
		return getRaftMachine().getState() != RaftState.STRANGER;
	}

	@Override
	public boolean isLeader() {
		return getRaftMachine().getState() == RaftState.LEADER;
	}

	@Override
	public boolean isFollower() {
		return getRaftMachine().getState() == RaftState.FOLLOWER;
	}

	@Override
	public List<LogEntry> getLog() {
		return getLogManager().getLogs(0, Integer.MAX_VALUE);
	}
}
