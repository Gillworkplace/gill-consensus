package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import com.gill.consensus.raftplus.mock.MockIntMapServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.gill.consensus.BaseTest;
import com.gill.consensus.raftplus.mock.MockNode;
import com.gill.consensus.raftplus.model.LogEntry;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONUtil;

/**
 * ClusterTest
 *
 * @author gill
 * @version 2023/09/04
 **/
@SpringBootTest
public class NodeTest extends BaseTest {

	private List<MockNode> nodesInit(int num, long waitTime) {
		List<MockNode> nodes = init(num);
		for (MockNode node : nodes) {
			node.start(nodes);
		}
		sleep(waitTime);
		assertCluster(nodes);
		return nodes;
	}

	private List<MockNode> nodesInitUntilStable(int num) {
		return nodesInitUntilStable(num, null);
	}

	private List<MockNode> nodesInitUntilStable(int num, Integer defaultPriority) {
		List<MockNode> nodes = init(num);
		for (MockNode node : nodes) {
			if (defaultPriority == null) {
				node.start(nodes);
			} else {
				node.start(nodes, defaultPriority);
			}
		}
		waitUtilStable(nodes);
		assertCluster(nodes);
		return nodes;
	}

	private static void assertCluster(List<MockNode> nodes) {
		int leaderCnt = 0;
		int followerCnt = 0;
		int availableCnt = 0;
		for (MockNode node : nodes) {
			if (node.isLeader()) {
				leaderCnt++;
			}
			if (node.isFollower()) {
				followerCnt++;
			}
			if (node.isUp()) {
				availableCnt++;
			}
		}
		try {
			Assertions.assertEquals(1, leaderCnt, "leader 数目异常");
			Assertions.assertEquals(availableCnt - 1, followerCnt, "follower 数目异常");
		} catch (Throwable e) {
			System.out.println("============ AFTER EXCEPTION ===============");
			stopNodes(nodes);
			throw e;
		}
	}

	private static void assertLogs(List<MockNode> nodes) {
		Optional<MockNode> leaderOpt = nodes.stream().filter(MockNode::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		MockNode leader = leaderOpt.get();
		List<LogEntry> logs = leader.getLog();
		String expected = JSONUtil.toJsonStr(logs);
		int cnt = 0;
		try {
			for (MockNode node : nodes) {
				if (expected.equals(JSONUtil.toJsonStr(node.getLog()))) {
					cnt++;
				}
			}
			Assertions.assertTrue(cnt > nodes.size() / 2);
		} catch (Throwable e) {
			System.out.println("============ AFTER EXCEPTION ===============");
			stopNodes(nodes);
			throw e;
		}
	}

	private static void waitUtilStable(List<MockNode> nodes) {
		while (true) {
			Optional<MockNode> leader = nodes.stream().filter(MockNode::isLeader).findFirst();
			if (leader.isPresent() && leader.get().isStable()) {
				break;
			}
			sleep(10);
		}
	}

	private static MockNode findLeader(List<MockNode> nodes) {
		Optional<MockNode> leaderOpt = nodes.stream().filter(MockNode::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		return leaderOpt.get();
	}

	@RepeatedTest(10)
	public void testNodeInit() {
		List<MockNode> nodes = nodesInit(1, 250);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 节点初始化在500ms内能选出主节点
	 */
	@RepeatedTest(50)
	public void testNodesInit() {
		List<MockNode> nodes = nodesInit(5, 500);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	@RepeatedTest(50)
	public void testNodesInitIfStable() {
		List<MockNode> nodes = nodesInitUntilStable(5);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	@RepeatedTest(50)
	public void testSamePriorityInitIfStable_Normal() {
		List<MockNode> nodes = nodesInitUntilStable(5, 0);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	@RepeatedTest(50)
	public void testSamePriorityInitIfStable_Extra() {
		List<MockNode> nodes = nodesInitUntilStable(21, 0);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 移除leader后能否重新选出节点
	 */
	@RepeatedTest(30)
	public void testRemoveLeader() {
		final int num = 5;
		List<MockNode> nodes = nodesInitUntilStable(num);
		Optional<MockNode> leaderOpt = nodes.stream().filter(MockNode::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		MockNode leader = leaderOpt.get();
		System.out.println("remove leader " + leader.getID());
		leader.stop();
		long start = System.currentTimeMillis();
		waitUtilStable(nodes);
		System.out.println("cost: " + (System.currentTimeMillis() - start));
		assertCluster(nodes);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 并发提交提案
	 */
	@RepeatedTest(30)
	public void testPropose() throws ExecutionException, InterruptedException {
		List<MockNode> nodes = nodesInitUntilStable(5);
		Optional<MockNode> leaderOpt = nodes.stream().filter(MockNode::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		MockNode leader = leaderOpt.get();
		System.out.println("============ PROPOSE =============");
		int concurrency = 20;
		CompletableFuture<?>[] futures = IntStream.range(0, concurrency)
				.mapToObj(x -> CompletableFuture.supplyAsync(() -> leader.propose(String.valueOf(x % 10))))
				.toArray(CompletableFuture[]::new);
		CompletableFuture.allOf(futures).join();
		for (int i = 0; i < futures.length; i++) {
			Assertions.assertNotEquals("-1", String.valueOf(futures[i].get()), "i: " + i);
		}
		assertLogs(nodes);
		System.out.println("============ TEST FINISHED =============");
		stopNodes(nodes);
	}

	/**
	 * 正常提交多次后，其中1台follower宕机，继续提交多次，最终状态一直。
	 */
	@Test
	public void testExceptionPropose() {
		List<MockNode> nodes = nodesInitUntilStable(5);
		MockNode leader = findLeader(nodes);
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

	private static void stopNodes(List<MockNode> nodes) {
		for (MockNode node : nodes) {
			node.stop();
		}
	}
}
