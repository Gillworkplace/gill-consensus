package com.gill.consensus.multipaxos;

import java.util.function.Supplier;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * AbstractState
 *
 * @author gill
 * @version 2023/08/01
 **/
@Setter
@Getter
@ToString(exclude = {"node"})
public abstract class AbstractState {

	protected final Node node;

	protected volatile int firstUnchosenIndex;

	public AbstractState(Node node) {
		this.node = node;
		node.setState(this);
	}

	/**
	 * lastLogIndex
	 */
	public Supplier<Integer> lastLogIndex = () -> getNode().getLogs().lastKey();

	/**
	 * nextState
	 *
	 * @return AbstractState
	 */
	public abstract AbstractState nextState();

	/**
	 * init
	 */
	public abstract void init();

	/**
	 * stop
	 */
	public abstract void stop();

	/**
	 * propose
	 *
	 * @param x
	 *            x
	 * @return boolean
	 */
	public abstract boolean propose(int x);
}
