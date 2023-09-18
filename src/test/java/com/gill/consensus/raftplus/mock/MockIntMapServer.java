package com.gill.consensus.raftplus.mock;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.example.intmap.IntMapServer;
import com.gill.consensus.raftplus.machine.RaftMachine;
import com.gill.consensus.raftplus.machine.RaftState;
import com.gill.consensus.raftplus.model.LogEntry;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

/**
 * MockIntMapServer
 *
 * @author gill
 * @version 2023/09/18
 **/
public class MockIntMapServer extends IntMapServer implements TestMethod {

    public MockIntMapServer(int id) {
        super(id);
    }

    public Node getNode() {
        return (Node) ReflectionTestUtils.getField(this, "node");
    }

    public RaftMachine getRaftMachine() {
        return (RaftMachine) ReflectionTestUtils.getField(getNode(), "machine");
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
        return getNode().getLogManager().getLogs(0, Integer.MAX_VALUE);
    }
}
