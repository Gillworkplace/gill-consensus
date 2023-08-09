package com.gill.consensus.raft;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.hutool.core.util.RandomUtil;
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
public class Follower extends AbstractState {

	private ScheduledExecutorService scheduler;

	private final int randomTime;

	public Follower(Node node) {
		super(node);
		randomTime = (node.getId() % 10) * 100 + RandomUtil.randomInt(0, 99);
		node.updateHeartBeat();
	}

	@Override
	public String toString() {
		return "Follower{}";
	}

	@Override
	public AbstractState start() {
		scheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "raft-follower"));
		scheduler.scheduleAtFixedRate(() -> {

			// 2个周期内没有收到leader的心跳包就升级成candidate参与leader竞选
			long now = System.currentTimeMillis();
			if (now - node.getHeartbeatTime() >= 2 * Custom.INTERVAL + randomTime) {
				this.upgrade();
			}
		}, 0, 50, TimeUnit.MILLISECONDS);
		return this;
	}

	@Override
	public AbstractState stop() {
		scheduler.shutdown();
		scheduler = null;
		return this;
	}

	@Override
	public AbstractState upgrade() {
		this.stop();
		log.debug("node {} is over election timeout, upgrade to candidate", node.getId());
		return new Candidate(node).start();
	}

	@Override
	public AbstractState downgrade() {
		this.stop();
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
