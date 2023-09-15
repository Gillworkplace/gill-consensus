package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.gill.consensus.BaseTest;
import com.gill.consensus.raftplus.mock.MockNode;
import com.gill.consensus.raftplus.model.LogEntry;

import cn.hutool.core.util.RandomUtil;

/**
 * ClusterTest
 *
 * @author gill
 * @version 2023/09/04
 **/
@SpringBootTest
public class ClusterTest extends BaseTest {

	private List<MockNode> nodesInit(int num) {
		List<MockNode> nodes = init(num);
		for (MockNode node : nodes) {
			node.start(nodes);
		}
		sleep(500);
		int leaderCnt = 0;
		int followerCnt = 0;
		for (MockNode node : nodes) {
			if (node.isLeader()) {
				leaderCnt++;
			}
			if (node.isFollower()) {
				followerCnt++;
			}
		}
		try {
			Assertions.assertEquals(1, leaderCnt, "leader 数目异常");
			Assertions.assertEquals(num - 1, followerCnt, "follower 数目异常");
		} catch (Throwable e) {
			System.out.println("============ AFTER EXCEPTION ===============");
			stopNodes(nodes);
			throw e;
		}
		return nodes;
	}

	@RepeatedTest(10)
	public void testNodeInit() {
		List<MockNode> nodes = nodesInit(1);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 节点初始化在500ms内能选出主节点
	 */
	@RepeatedTest(30)
	public void testNodesInit() {
		List<MockNode> nodes = nodesInit(5);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 移除leader后能否重新选出节点
	 */
	@RepeatedTest(10)
	public void testRemoveLeader() {
		final int num = 5;
		List<MockNode> nodes = nodesInit(num);
		Optional<MockNode> leaderOpt = nodes.stream().filter(MockNode::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		MockNode leader = leaderOpt.get();
		System.out.println("remove leader...");
		leader.stop();
		sleep(500);
		int leaderCnt = 0;
		for (MockNode node : nodes) {
			if (node.isLeader()) {
				leaderCnt++;
			}
		}
		try {
			Assertions.assertEquals(1, leaderCnt, "leader 数目异常");
			stopNodes(nodes);
		} catch (Throwable e) {
			stopNodes(nodes);
			throw e;
		}
	}

	/**
	 * 并发提交提案
	 */
	@Test
	public void testPropose() {
		List<MockNode> nodes = nodesInit(5);
		MockNode first = nodes.stream().findFirst().get();
		int concurrency = 20;
		ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
		for (int i = 0; i < concurrency; i++) {
			final int x = i;
			executorService.execute(() -> Assertions.assertTrue(first.propose(String.valueOf(x % 10)) >= 0));
		}
		sleep(500);
		List<LogEntry> logs = first.getLog();
		for (MockNode node : nodes) {
			Assertions.assertEquals(logs, node.getLog(), "node: " + node.getID());
		}
	}

	/**
	 * 正常提交多次后，其中1台follower宕机，继续提交多次，最终状态一直。
	 */
	@Test
	public void testExceptionPropose() {
		List<MockNode> nodes = nodesInit(5);
		MockNode leader = nodes.stream().filter(MockNode::isLeader).findFirst().get();
		leader.propose("1");
		leader.propose("2");
		leader.propose("3");
		MockNode follower = nodes.stream().filter(MockNode::isFollower).findFirst().get();
		follower.stop();
		leader.propose("3");
		leader.propose("4");
		leader.propose("5");
		follower.start(nodes);
		leader.propose("1");
		leader.propose("5");
		sleep(500);
		List<LogEntry> logs = leader.getLog();
		for (MockNode node : nodes) {
			Assertions.assertEquals(logs, node.getLog());
		}
	}

	private List<MockNode> init(int num) {
		List<MockNode> nodes = new ArrayList<>();
		int offset = RandomUtil.randomInt(1000) * 100;
		for (int i = 0; i < num; i++) {
			nodes.add(new MockNode(offset + i));
		}
		System.out.println("offset: " + offset);
		return nodes;
	}

	private static void printNodes(List<MockNode> nodes) {
		for (MockNode node : nodes) {
			System.out.println(node.println());
		}
	}

	private static void stopNodes(List<MockNode> nodes) {
		for (MockNode node : nodes) {
			node.stop();
		}
	}
}
