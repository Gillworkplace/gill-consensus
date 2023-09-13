package com.gill.consensus.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.gill.consensus.BaseTest;
import com.gill.consensus.common.Util;
import com.gill.consensus.raft.mock.NodeRemote;
import com.gill.consensus.raft.model.LogEntry;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * RaftTest
 *
 * @author gill
 * @version 2023/08/05
 **/
@Slf4j
@SpringBootTest
public class RaftTest extends BaseTest {

	private List<Node> init(int size) {
		List<Node> nodes = createNodes(size);
		nodes.forEach(Node::start);
		log.info("start");
		sleep(1000L);
		log.info("print");
		print(nodes);
		checkOnlyOneLeader(nodes);
		return nodes;
	}

	/**
	 * 测试初始节点是否能选出Leader
	 */
	@Test
	public void testElection() {
		init(5);
	}

	@RepeatedTest(10)
	public void testElectionSmall() {
		init(3);
	}

	@RepeatedTest(10)
	public void testElectionNormal() {
		init(10);
	}

	/**
	 * 测试propose
	 */
	@Test
	public void testPropose() {
		List<Node> nodes = init(5);
		boolean propose = nodes.get(0).propose(123, 61);
		print(nodes);
		Assert.isTrue(propose, "propose failed");
		Node leader = leader(nodes);
		Assert.equals(61, Optional.ofNullable(leader.getLogs().get(2)).map(entry -> entry.val).orElse(-1));
		checkLogsConsistency(nodes);
	}

	@RepeatedTest(10)
	public void testProposes() {
		testPropose();
	}

	/**
	 * 测试并发propose
	 */
	@Test
	public void testProposeConcurrently() {
		List<Node> nodes = init(5);
		Util.concurrencyCall(IntStream.range(0, 10).boxed().collect(Collectors.toList()),
				idx -> Assert.isTrue(nodes.get(RandomUtil.randomInt(0, 5)).propose(RandomUtil.randomInt(), idx)));
		print(nodes);
		Node leader = leader(nodes);
		Assert.equals(12, leader.getLogs().size());
		checkLogsConsistency(nodes);
	}

	@RepeatedTest(10)
	public void testProposesConcurrently() {
		testProposes();
	}

	/**
	 * 测试并发propose
	 */
	@Test
	public void testProposeRemoteConcurrently() {
		List<Node> nodes = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			nodes.add(newTarget(NodeRemote.class));
		}
		for (Node node : nodes) {
			node.setNodes(nodes);
		}
		nodes.forEach(Node::start);
		log.info("start");
		sleep(1000L);
		log.info("print");
		print(nodes);
		checkOnlyOneLeader(nodes);
		Util.concurrencyCall(IntStream.range(0, 10).boxed().collect(Collectors.toList()),
				idx -> Assert.isTrue(nodes.get(RandomUtil.randomInt(0, 5)).propose(RandomUtil.randomInt(), idx)));
		print(nodes);
		Node leader = leader(nodes);
		Assert.equals(12, leader.getLogs().size());
		checkLogsConsistency(nodes);
	}

	/**
	 * 测试leader日志比follower新的日志同步
	 */
	@Test
	public void testLeaderOverFollower() {
		List<Node> leaders = newList(1, "raftNode", Node.class);
		Node leader = leaders.get(0);
		leader.start();
		sleep(1000);
		Assert.isTrue(leader.propose(123, 321), "leader propose failed");

		// 追加follower
		List<Node> nodes = newList(2, Node.class);
		nodes.forEach(Node::start);
		nodes.add(leader);
		print(nodes);
		for (Node node : nodes) {
			node.setNodes(nodes);
		}
		Assert.isTrue(leader.propose(123, 321), "leader propose 2nd failed");
		print(nodes);
		checkOnlyOneLeader(nodes);
	}

	@Test
	public void testLeaderMoreOverFollower() {
		List<Node> leaders = newList(1, "raftNode", Node.class);
		Node leader = leaders.get(0);
		leader.start();
		sleep(1000);
		for (int i = 0; i < 20; i++) {
			Assert.isTrue(leader.propose(i + 100, i), "leader propose failed");
		}

		// 追加follower
		List<Node> nodes = newList(2, Node.class);
		nodes.forEach(Node::start);
		nodes.add(leader);
		print(nodes);
		for (Node node : nodes) {
			node.setNodes(nodes);
		}
		Assert.isTrue(leader.propose(123, 321), "leader propose 2nd failed");
		print(nodes);
		checkOnlyOneLeader(nodes);
	}

	private static void checkOnlyOneLeader(List<Node> nodes) {
		int leaderCnt = 0;
		int followerCnt = 0;
		for (Node node : nodes) {
			if (node.isUp()) {
				if (node.getState() instanceof Leader) {
					leaderCnt++;
				} else if (node.getState() instanceof Follower) {
					followerCnt++;
				}
			}
		}
		Assert.isTrue(leaderCnt == 1, "leader cnt error");
		Assert.isTrue(followerCnt == nodes.size() - 1, "follower cnt error");
	}

	private List<Node> createNodes(int size) {
		List<Node> nodes = newList(size, "raftNode", Node.class);
		for (Node node : nodes) {
			node.setNodes(nodes);
		}
		print(nodes);
		return nodes;
	}

	private Node leader(List<Node> nodes) {
		return nodes.stream().filter(node -> node.getState() instanceof Leader).findFirst().get();
	}

	private void checkLogsConsistency(List<Node> nodes) {
		ConcurrentSkipListMap<Integer, LogEntry> base = nodes.get(0).getLogs();
		for (Node node : nodes) {
			ConcurrentSkipListMap<Integer, LogEntry> logs = node.getLogs();
			if (base.size() != logs.size()) {
				Assert.equals(base, logs);
			}
		}
	}
}
