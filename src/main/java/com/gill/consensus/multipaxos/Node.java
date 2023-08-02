package com.gill.consensus.multipaxos;

import static com.gill.consensus.common.Util.CHOSEN;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.common.Util;

import cn.hutool.core.lang.Pair;
import lombok.Getter;
import lombok.Setter;

/**
 * Node
 *
 * @author gill
 * @version 2023/08/01
 **/
@Getter
@Setter
@Scope("prototype")
@Component("multiNode")
public class Node {

	private final ConcurrentSkipListMap<Integer, Pair<Integer, Integer>> logs = new ConcurrentSkipListMap<>(
			Comparator.comparingInt(idx -> idx));

	{
		logs.put(0, Pair.of(CHOSEN, 0));
	}

	private volatile boolean up = true;

	private final int id = Util.getId();

	private Node leader;

	private List<Node> others = new ArrayList<>();

	private AbstractState state = new Acceptor(this);

	/**
	 * init
	 */
	public void init() {
		up = true;
		state.init();
	}

	/**
	 * stop
	 */
	public void stop() {
		state.stop();
		up = false;
	}

	/**
	 * ping
	 *
	 * @return int
	 */
	public int ping() {
		return id;
	}

	/**
	 * findMaxNode
	 *
	 * @return Node
	 */
	public Node findMaxNode() {
		AtomicReference<Node> maxNode = new AtomicReference<>(this);
		Util.concurrencyCall(others, node -> maxNode.accumulateAndGet(node, (prev, n) -> {
			if (n.isUp() && n.ping() > prev.getId()) {
				return n;
			}
			return prev;
		}));
		return maxNode.get();
	}

	/**
	 * propose
	 *
	 * @param x
	 *            x
	 * @return boolean
	 */
	public boolean propose(int x) {
		return this.state.propose(x);
	}

	@Override
	public String toString() {
		return "Node{" + "id=" + id + ", up=" + up + ", leader=" + (leader == null ? "null" : leader.getId())
				+ ", others=" + others.stream().map(Node::getId).collect(Collectors.toList()) + ", state=" + state
				+ ", logs=" + logs + '}';
	}
}
