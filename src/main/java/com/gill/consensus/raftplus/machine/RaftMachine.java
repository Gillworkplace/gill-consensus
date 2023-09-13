package com.gill.consensus.raftplus.machine;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gill.consensus.raftplus.Node;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;

import javafx.util.Pair;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * StateMachine
 *
 * @author gill
 * @version 2023/08/18
 **/
@Slf4j
public class RaftMachine {

	private final HashBasedTable<RaftState, RaftEvent, Target> table = HashBasedTable.create();

	private final BlockingDeque<Pair<RaftEvent, RaftEventParams>> eventQueue = new LinkedBlockingDeque<>();

	private final ExecutorService executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "raft-machine"),
			(r, executor) -> log.warn("raft machine receive another scheduler"));

	{
		table.put(RaftState.STRANGER, RaftEvent.INIT, Target.of(ImmutableList.of(RaftAction.INIT), RaftState.FOLLOWER,
				ImmutableList.of(RaftAction.POST_FOLLOWER)));

		table.put(RaftState.FOLLOWER, RaftEvent.PING_TIMEOUT,
				Target.of(ImmutableList.of(RaftAction.REMOVE_FOLLOWER_SCHEDULER), RaftState.PRE_CANDIDATE,
						ImmutableList.of(RaftAction.TO_PRE_CANDIDATE)));
		table.put(RaftState.FOLLOWER, RaftEvent.ACCEPT_LEADER, Target.of(ImmutableList.of(RaftAction.ACCEPT_LEADER),
				RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.FOLLOWER, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.REMOVE_FOLLOWER_SCHEDULER, RaftAction.STOP), RaftState.STRANGER));

		table.put(RaftState.PRE_CANDIDATE, RaftEvent.PREVOTE_SUCCESS,
				Target.of(RaftState.CANDIDATE, ImmutableList.of(RaftAction.POST_CANDIDATE)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.PREVOTE_FAILED,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.ACCEPT_LEADER,
				Target.of(ImmutableList.of(RaftAction.ACCEPT_LEADER), RaftState.FOLLOWER,
						ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.FORCE_FOLLOWER,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.STOP), RaftState.STRANGER));

		table.put(RaftState.CANDIDATE, RaftEvent.TO_LEADER,
				Target.of(RaftState.LEADER, ImmutableList.of(RaftAction.POST_LEADER)));
		table.put(RaftState.CANDIDATE, RaftEvent.VOTE_FAILED,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.CANDIDATE, RaftEvent.ACCEPT_LEADER, Target.of(ImmutableList.of(RaftAction.ACCEPT_LEADER),
				RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.CANDIDATE, RaftEvent.FORCE_FOLLOWER,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.CANDIDATE, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.STOP), RaftState.STRANGER));

		table.put(RaftState.LEADER, RaftEvent.ACCEPT_LEADER,
				Target.of(ImmutableList.of(RaftAction.REMOVE_LEADER_SCHEDULER, RaftAction.ACCEPT_LEADER),
						RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.LEADER, RaftEvent.NETWORK_PARTITION,
				Target.of(ImmutableList.of(RaftAction.REMOVE_LEADER_SCHEDULER), RaftState.FOLLOWER,
						ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.LEADER, RaftEvent.FORCE_FOLLOWER,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.LEADER, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.REMOVE_LEADER_SCHEDULER, RaftAction.STOP), RaftState.STRANGER));
	}

	private final Node node;

	private RaftState state;

	public RaftMachine(Node node) {
		this.node = node;
		this.state = RaftState.STRANGER;
	}

	public RaftState getState() {
		return state;
	}

	/**
	 * 发布事件
	 * 
	 * @param event
	 *            事件
	 * @param params
	 *            参数
	 */
	public void publishEvent(RaftEvent event, RaftEventParams params) {
		Pair<RaftEvent, RaftEventParams> pair = new Pair<>(event, params);

		// 该事件优先级最高并可中断其他事件
		if (event == RaftEvent.ACCEPT_LEADER) {
			eventQueue.offerFirst(pair);
		} else {
			eventQueue.offerLast(pair);
		}
	}

	/**
	 * 启动
	 *
	 * @return this
	 */
	@SuppressWarnings({"InfiniteLoopStatement", "ConstantConditions"})
	public RaftMachine start() {

		// 该方法为单线程执行，没有线程安全问题
		executor.execute(() -> {
			while (true) {
				try {
					Pair<RaftEvent, RaftEventParams> eventPair = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
					RaftEvent event = eventPair.getKey();
					RaftEventParams params = eventPair.getValue();
					Target target = table.get(state, eventPair.getKey());

					// 状态和事件对不上说明优先级高的Accept Leader事件被处理了，旧的事件可以抛弃不处理了。
					if (target == null) {
						log.info("ignore event. current term {}, event: {}, event term: {}", node.getTerm(), event,
								params.getTerm());
						continue;
					}
					if (node.getTerm() > params.getTerm()) {
						log.info("discard event. current term {}, event: {}, event term: {}", node.getTerm(), event,
								params.getTerm());
						continue;
					}

					// 执行pre动作
					preActions(event, params, target);

					// 转换状态
					this.state = target.getTargetState();
					log.debug("node: {} change to {}", node.getID(), state.name());

					// 执行post动作
					postActions(event, params, target);
				} catch (InterruptedException e) {
					log.error(e.toString());
				}
			}
		});
		return this;
	}

	private void preActions(RaftEvent event, RaftEventParams params, Target target) {
		List<RaftAction> preActions = target.getPreActions();
		for (RaftAction action : preActions) {
			try {
				log.trace("state: {} receives event: {} to do pre-action: {}", state, event.name(), action.name());
				action.action(node, params);
			} catch (Exception e) {
				log.error("node: {} state {} action event {} failed, e: {}", node.getID(), state.name(), event.name(),
						e);
			}
		}
	}

	private void postActions(RaftEvent event, RaftEventParams params, Target target) {
		List<RaftAction> postActions = target.getPostActions();
		for (RaftAction action : postActions) {
			try {
				log.trace("state: {} receives event: {} to do post-action: {}", state, event.name(), action.name());
				action.action(node, params);
			} catch (Exception e) {
				log.error("node: {} state {} action event {} failed, e: {}", node.getID(), state.name(), event.name(),
						e);
			}
		}
	}

	/**
	 * 状态机是否继续
	 * 
	 * @return 是否继续
	 */
	public boolean isReady() {
		return this.state != RaftState.STRANGER;
	}

	@Getter
	private static class Target {

		private final List<RaftAction> preActions;

		private final RaftState targetState;

		private final List<RaftAction> postActions;

		private Target(List<RaftAction> preActions, RaftState targetState, List<RaftAction> postActions) {
			this.preActions = preActions;
			this.targetState = targetState;
			this.postActions = postActions;
		}

		public static Target of(List<RaftAction> preAction, RaftState targetState, List<RaftAction> postAction) {
			return new Target(preAction, targetState, postAction);
		}

		public static Target of(RaftState targetState, List<RaftAction> postAction) {
			return new Target(Collections.emptyList(), targetState, postAction);
		}

		public static Target of(List<RaftAction> preAction, RaftState targetState) {
			return new Target(preAction, targetState, Collections.emptyList());
		}

		public static Target of(RaftState targetState) {
			return new Target(Collections.emptyList(), targetState, Collections.emptyList());
		}
	}
}
