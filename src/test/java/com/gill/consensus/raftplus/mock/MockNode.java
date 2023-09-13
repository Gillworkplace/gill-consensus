package com.gill.consensus.raftplus.mock;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.machine.RaftState;
import com.gill.consensus.raftplus.model.LogEntry;
import com.gill.consensus.raftplus.service.InnerNodeService;

import java.util.List;
import java.util.Map;

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
