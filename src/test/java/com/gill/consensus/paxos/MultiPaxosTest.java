package com.gill.consensus.paxos;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.gill.consensus.BaseTest;
import com.gill.consensus.common.Util;
import com.gill.consensus.multipaxos.Node;

/**
 * MultiPaxosTest
 *
 * @author gill
 * @version 2023/08/01
 **/
@SpringBootTest
public class MultiPaxosTest extends BaseTest {

	private List<Node> createNodes(int size) {
		List<Node> nodes = newList(5, Node.class);
		print(nodes);
		printSplit();
		return nodes;
	}

	private void initNode(Node node, List<Node> all) {
		node.setOthers(all.stream().filter(n -> n != node).collect(Collectors.toList()));
	}

	@Test
	public void testInitNodes() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(1000L);
		print(nodes);
		printSplit();
	}

	@Test
	public void testAddLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);
		print(nodes);
		printSplit();

		Node newLeader = newTarget(Node.class);
		nodes.add(newLeader);
		for (Node node : nodes) {
			node.getOthers().add(newLeader);
		}
		initNode(newLeader, nodes);
		sleep(500L);
		print(nodes);
	}

	@Test
	public void testRemoveLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);
		print(nodes);
		printSplit();

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(Node::stop);
		sleep(500L);
		print(nodes);
	}

	@Test
	public void testRemoveAndThenAddLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);
		print(nodes);
		printSplit();

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(node -> {
			node.stop();
			sleep(500L);
			print(nodes);
			printSplit();

			node.init();
			sleep(500L);
			print(nodes);
		});
	}

	@Test
	public void testLeaderPropose() {
		List<Node> nodes = createNodes(4);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(node -> node.propose(10));
		print(nodes);
	}

	@Test
	public void testLeaderProposesSequence() {
		List<Node> nodes = createNodes(4);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(node -> {
			for (int i = 100; i < 105; i++) {
				node.propose(i);
			}
		});
		print(nodes);
	}

	@Test
	public void testLeaderProposesConcurrency() {
		List<Node> nodes = createNodes(4);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(500L);

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(node -> Util
				.concurrencyCall(IntStream.range(100, 105).boxed().collect(Collectors.toList()), node::propose));
		print(nodes);
	}
}
