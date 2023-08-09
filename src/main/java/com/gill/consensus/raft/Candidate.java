package com.gill.consensus.raft;

import static com.gill.consensus.common.Util.CPU_CORES;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gill.consensus.common.Util;
import com.gill.consensus.raft.model.LogEntry;
import com.gill.consensus.raft.model.Vote;

import cn.hutool.core.lang.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Leader
 *
 * @author gill
 * @version 2023/08/02
 **/
@Slf4j
@Getter
@Setter
public class Candidate extends AbstractState {

	private ExecutorService pool = new ThreadPoolExecutor(4 * CPU_CORES + 1, 4 * CPU_CORES + 1, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "raft-candidate"));

	public Candidate(Node node) {
		super(node);
	}

	@Override
	public String toString() {
		return "Candidate{}";
	}

	@Override
	public AbstractState start() {

		// 如果获取不到锁则回到follower等待
		if (!node.VOTE_LOCK.tryLock()) {
			log.debug("node {} grant vote lock failed", node.getId());
			return this.downgrade();
		}

		log.debug("node {} get candidate vote lock", node.getId());

		// 选举自己
		try {
			node.getCurrentTerm().incrementAndGet();
			node.setVotedFor(node.getId());
			List<Node> followers = Util.getFollowers(node.getNodes(), n -> node.getId() != n.getId());
			boolean ret = Util.majorityCall(followers, follower -> {
				Pair<Integer, LogEntry> lastNode = node.getLastNode();
				Vote vote = new Vote(node.getCurrentTerm().get(), node.getId(), lastNode.getKey(),
						lastNode.getValue().term);
				return follower.requestVote(vote);
			}, voteReply -> {
				if (voteReply.success) {
					return true;
				}
				node.getCurrentTerm().accumulateAndGet(voteReply.term, (prev, n) -> Math.max(n, prev));
				return false;
			}, pool, "requestVote");

			if (ret) {

				// 投票成功 升级成 leader
				return this.upgrade();
			} else {

				// 投票失败 降级为 follower 等待选举超时触发 重新选举
				return this.downgrade();
			}
		} finally {
			log.debug("node {} release vote lock", node.getId());
			node.VOTE_LOCK.unlock();
		}
	}

	@Override
	public AbstractState stop() {
		pool.shutdown();
		pool = null;
		return this;
	}

	@Override
	public AbstractState upgrade() {
		this.stop();
		log.debug("node {} term {} is elected to leader", node.getId(), node.getCurrentTerm().get());
		return new Leader(node).start();
	}

	@Override
	public AbstractState downgrade() {
		this.stop();
		log.debug("node {} elect failed", node.getId());
		return new Follower(node).start();
	}

	@Override
	public boolean propose(int xid, int x) {
		return Optional.ofNullable(node.getLeader()).map(leader -> {
			log.debug("transpond to leader: {}", leader.getId());
			return leader.propose(xid, x);
		}).orElse(false);
	}

}
