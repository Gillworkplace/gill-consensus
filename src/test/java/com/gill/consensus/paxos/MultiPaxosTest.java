package com.gill.consensus.paxos;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

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
		sleep(2000L);
		print(nodes);
		printSplit();
	}

	@Test
	public void testAddLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(2000L);
		print(nodes);
		printSplit();

		Node newLeader = newTarget(Node.class);
		nodes.add(newLeader);
		for (Node node : nodes) {
			node.getOthers().add(newLeader);
		}
		initNode(newLeader, nodes);
		sleep(2000L);
		print(nodes);
	}

	@Test
	public void testRemoveLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(2000L);
		print(nodes);
		printSplit();

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(Node::stop);
		sleep(2000L);
		print(nodes);
	}

	@Test
	public void testRemoveAndThenAddLeader() {
		List<Node> nodes = createNodes(5);
		for (Node node : nodes) {
			initNode(node, nodes);
		}
		sleep(2000L);
		print(nodes);
		printSplit();

		nodes.stream().max(Comparator.comparingInt(Node::getId)).ifPresent(node -> {
			node.stop();
			sleep(2000L);
			print(nodes);
			printSplit();

			node.init();
			sleep(2000L);
			print(nodes);
		});
	}
}
