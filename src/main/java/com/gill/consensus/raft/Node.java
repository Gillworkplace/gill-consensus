package com.gill.consensus.raft;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.gill.consensus.common.Cost;
import com.gill.consensus.common.Util;
import com.gill.consensus.raft.model.LogEntries;
import com.gill.consensus.raft.model.LogEntry;
import com.gill.consensus.raft.model.Reply;
import com.gill.consensus.raft.model.Vote;
import com.gill.consensus.raft.model.VoteReply;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.lang.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Node
 *
 * @author gill
 * @version 2023/08/02
 **/
@Slf4j
@Setter
@Getter
@Scope("prototype")
@Component("raftNode")
public class Node {

	public final Lock VOTE_LOCK = new ReentrantLock();

	private final int id = Util.getId();

	private final Lock termLock = new ReentrantLock();

	private volatile boolean up = true;

	private volatile long heartbeatTime = System.currentTimeMillis();

	private AtomicInteger currentTerm = new AtomicInteger(0);

	private volatile int votedFor;

	private AbstractState state = new Follower(this);

	private Node leader;

	private List<Node> nodes = new ArrayList<>();

	private List<NodeProxy> nodesProxy = new ArrayList<>();

	private final ConcurrentSkipListMap<Integer, LogEntry> logs = new ConcurrentSkipListMap<>(
			Comparator.comparingInt(i -> i));

	{
		logs.put(0, new LogEntry(0, -1, 0, -1));
	}

	private AtomicInteger nextIndex = new AtomicInteger(logs.lastKey() + 1);

	private AtomicInteger committedIndex = new AtomicInteger(0);

	private Set<Integer> visited = new ConcurrentHashSet<>();

	/**
	 * start
	 */
	public void start() {
		up = true;
		state.start();
	}

	/**
	 * stop
	 */
	public void stop() {
		up = false;
		state.stop();
	}

	public synchronized void setNodes(List<Node> nodes) {
		this.nodes = nodes;
		this.nodesProxy = nodes.stream().map(node -> new NodeProxy(this, node)).collect(Collectors.toList());
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public List<NodeProxy> getNodesProxy() {
		return nodesProxy;
	}

	public void updateHeartBeat() {
		this.heartbeatTime = System.currentTimeMillis();
	}

	/**
	 * getLastNode
	 * 
	 * @return Pair
	 */
	public Pair<Integer, LogEntry> getLastNode() {
		Map.Entry<Integer, LogEntry> entry = logs.lastEntry();
		return Pair.of(entry.getKey(), entry.getValue());
	}

	/**
	 * propose
	 * 
	 * @param xid
	 *            xid
	 * @param x
	 *            x
	 * @return boolean
	 */
	@Cost
	public boolean propose(int xid, int x) {
		return state.propose(xid, x);
	}

	/**
	 * vote
	 * 
	 * @param vote
	 *            vote
	 * @return VoteReply
	 */
	@Cost(threshold = 0)
	public VoteReply requestVote(Vote vote) {
		VoteReply reply = new VoteReply();
		log.debug("node {} receive vote request: {}", this.id, vote);
		VOTE_LOCK.lock();
		try {
			int term = currentTerm.get();

			// 投票响应的任期为投票者投票前的任期
			reply.term = term;
			reply.success = false;

			// 任期太旧直接返回
			if (vote.term < term) {
				return reply;
			}

			if (vote.term > term) {
				currentTerm.set(vote.term);

				// 降级成为 follower
				state.downgrade();
				votedFor = 0;
			}

			// 任期内只投一次，并且幂等 TODO 增加日志完整性判定
			if (votedFor == 0 || votedFor == vote.id) {

				// 日志版本判断，成为leader的节点必须保证其日志版本比过半的节点要新
				// 比较优先级：任期 > 日志序号
				Pair<Integer, LogEntry> lastNode = getLastNode();
				if (vote.lastLogTerm < lastNode.getValue().term || vote.lastLogIndex < lastNode.getKey()) {
					return reply;
				}

				// 投票成功，相当于认该Candidate为Leader，因此更新心跳时间，这也是减少已投票的节点立刻触发参与候选而导致增加候选冲突的几率。
				reply.success = true;
				votedFor = vote.id;
				updateHeartBeat();
			}
			return reply;
		} finally {
			VOTE_LOCK.unlock();
		}
	}

	/**
	 * appendLogEntries
	 * 
	 * @param logEntries
	 *            logEntries
	 * @return Reply
	 */
	@Cost
	public Reply appendLogEntries(LogEntries logEntries) {
		Reply reply = new Reply();
		reply.success = false;
		reply.term = this.currentTerm.get();
		if (logEntries.node == this) {
			reply.success = true;
			return reply;
		}

		if (logEntries.term < currentTerm.get()) {
			return reply;
		}

		// 更新心跳时间
		updateHeartBeat();

		VOTE_LOCK.lock();
		try {

			// 因为一个任期最多只会有一个Leader，所以收到更高的任期时说明有新的leader选举成功
			if (logEntries.term > currentTerm.get()) {
				reply.term = logEntries.term;
				if (!(this.state instanceof Follower)) {
					this.state.downgrade();
				}
				currentTerm.set(logEntries.term);
				this.leader = logEntries.node;
				this.votedFor = this.leader.getId();
			}

			// logEntries 为null 代表该请求属于ping类型，不需要进行同步日志
			if (logEntries.logEntries == null) {
				leader = logEntries.node;
				reply.success = true;
				return reply;
			}

			// 日志一致性检查
			Pair<Integer, LogEntry> lastNode = getLastNode();
			int lastLogIndex = lastNode.getKey();
			int lastLogTerm = lastNode.getValue().term;

			// 1.如果leader的前一条日志序号大于follower节点的最大日志序号，说明follower节点的日志很旧，需要leader找出相应进度的preLogIndex的日志。
			// 2.如果版本号不一致，说明leader发生过变更，因此需要重新同步日志
			if (logEntries.prevLogIndex > lastLogIndex || logEntries.prevLogTerm != lastLogTerm) {
				return reply;
			}

			for (LogEntry logEntry : logEntries.logEntries) {
				int idx = logEntry.index;

				// 幂等性处理
				if (idx < lastLogIndex && logEntry.term == logs.get(idx).term) {
					continue;
				}
				logs.put(idx, logEntry);
			}

			// 上述接受了日志，因此需要重新获取一下最后一条日志
			lastNode = getLastNode();
			lastLogIndex = lastNode.getKey();

			// 应用提交的日志
			int committedIdx = committedIndex.get();
			if (committedIdx < logEntries.committedIndex) {
				int startIndex = committedIdx + 1;
				committedIndex.accumulateAndGet(Math.min(logEntries.committedIndex, lastLogIndex),
						(prev, n) -> Math.max(n, prev));
				apply(startIndex);
			}

			// 更新心跳时间
			reply.success = true;
			updateHeartBeat();
			return reply;
		} finally {
			VOTE_LOCK.unlock();
		}
	}

	private void apply(int startIdx) {

		// TODO 应用日志

	}

	@Override
	public String toString() {
		return "Node{" + "id=" + id + ", up=" + up + ", heartbeatTime=" + heartbeatTime + ", currentTerm="
				+ currentTerm.get() + ", votedFor=" + votedFor + ", leader=" + leader + ", logs=" + logs
				+ ", nextIndex=" + nextIndex + ", committedIndex=" + committedIndex + ", visited=" + visited
				+ ", state=" + state + '}';
	}
}
