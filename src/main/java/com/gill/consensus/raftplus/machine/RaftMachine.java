package com.gill.consensus.raftplus.machine;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gill.consensus.raftplus.Node;
import com.gill.consensus.raftplus.service.PrintService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * StateMachine
 *
 * @author gill
 * @version 2023/08/18
 **/
@Slf4j
public class RaftMachine implements PrintService {

	private final HashBasedTable<RaftState, RaftEvent, Target> table = HashBasedTable.create();

	private final BlockingDeque<RaftEntry> eventQueue = new LinkedBlockingDeque<>();

	private final Node node;

	private final ExecutorService executor;

	private volatile boolean running = true;

	{
		table.put(RaftState.STRANGER, RaftEvent.INIT, Target.of(ImmutableList.of(RaftAction.INIT), RaftState.FOLLOWER,
				ImmutableList.of(RaftAction.POST_FOLLOWER)));

		table.put(RaftState.FOLLOWER, RaftEvent.PING_TIMEOUT,
				Target.of(ImmutableList.of(RaftAction.REMOVE_FOLLOWER_SCHEDULER), RaftState.PRE_CANDIDATE,
						ImmutableList.of(RaftAction.TO_PRE_CANDIDATE)));
		table.put(RaftState.FOLLOWER, RaftEvent.STOP, Target.of(
				ImmutableList.of(RaftAction.PRE_STOP, RaftAction.REMOVE_FOLLOWER_SCHEDULER, RaftAction.CLEAR_POOL),
				RaftState.STRANGER, ImmutableList.of(RaftAction.POST_STOP)));

		table.put(RaftState.PRE_CANDIDATE, RaftEvent.TO_CANDIDATE,
				Target.of(RaftState.CANDIDATE, ImmutableList.of(RaftAction.POST_CANDIDATE)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.FORCE_FOLLOWER,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.PRE_CANDIDATE, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.PRE_STOP, RaftAction.CLEAR_POOL), RaftState.STRANGER,
						ImmutableList.of(RaftAction.POST_STOP)));

		table.put(RaftState.CANDIDATE, RaftEvent.TO_LEADER, Target.of(ImmutableList.of(RaftAction.INIT_LEADER),
				RaftState.LEADER, ImmutableList.of(RaftAction.POST_LEADER)));
		table.put(RaftState.CANDIDATE, RaftEvent.FORCE_FOLLOWER,
				Target.of(RaftState.FOLLOWER, ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.CANDIDATE, RaftEvent.STOP,
				Target.of(ImmutableList.of(RaftAction.PRE_STOP, RaftAction.CLEAR_POOL), RaftState.STRANGER,
						ImmutableList.of(RaftAction.POST_STOP)));

		table.put(RaftState.LEADER, RaftEvent.FORCE_FOLLOWER,
				Target.of(ImmutableList.of(RaftAction.REMOVE_LEADER_SCHEDULER), RaftState.FOLLOWER,
						ImmutableList.of(RaftAction.POST_FOLLOWER)));
		table.put(RaftState.LEADER, RaftEvent.STOP, Target.of(ImmutableList.of(RaftAction.PRE_STOP), RaftState.STRANGER,
				ImmutableList.of(RaftAction.REMOVE_LEADER_SCHEDULER, RaftAction.CLEAR_POOL, RaftAction.POST_STOP)));
	}

	private RaftState state;

	public RaftMachine(Node node) {
		this.node = node;
		this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(Collections.emptyList()), r -> new Thread(r, "machine-" + node.getID()),
				(r, executor) -> log.warn("raft machine receive another scheduler"));
		this.state = RaftState.STRANGER;
	}

	public RaftState getState() {
		return state;
	}

	/**
	 * 启动
	 *
	 * @return this
	 */
	public RaftMachine start() {
		log.info("machine is starting");
		eventQueue.clear();

		// 该方法为单线程执行，没有线程安全问题
		executor.execute(() -> {
			while (running) {
				try {
					RaftEntry entry = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
					if (entry == null) {
						continue;
					}
					RaftEvent event = entry.getEvent();
					RaftEventParams params = entry.getParams();
					Target target = table.get(state, event);

					// 状态和事件对不上说明优先级高的Accept Leader事件被处理了，旧的事件可以抛弃不处理了。
					if (target == null) {
						log.trace("ignore event {}. current state {}", event, state);
						tagCompleted(entry, params);
						continue;
					}
					if (node.getTerm() > params.getTerm()) {
						log.debug("discard event {}. current term {}, event term: {}", event, node.getTerm(),
								params.getTerm());
						tagCompleted(entry, params);
						continue;
					}

					// 执行pre动作
					preActions(event, params, target);

					// 转换状态
					this.state = target.getTargetState();
					log.debug("to be {}", state.name());

					// 执行post动作
					postActions(event, params, target);
					tagCompleted(entry, params);
				} catch (InterruptedException e) {
					log.warn(e.toString());
				} catch (Exception e) {
					log.error("machine exception: {}", e.toString());
				}
			}
			log.info("machine exits");
		});
		return this;
	}

	/**
	 * 停止machine
	 */
	public void stop() {
		log.info("machine is stopping");
		running = false;
	}

	private void preActions(RaftEvent event, RaftEventParams params, Target target) {
		List<RaftAction> preActions = target.getPreActions();
		for (RaftAction action : preActions) {
			try {
				log.trace("state: {} receives event: {} to do pre-action: {}", state, event.name(), action.name());
				action.action(node, params);
			} catch (Exception e) {
				log.error("state {} action event {} failed, e: {}", state.name(), event.name(), e);
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
				log.error("state {} action event {} failed, e: {}", state.name(), event.name(), e);
			}
		}
	}

	private void tagCompleted(RaftEntry entry, RaftEventParams params) {
		entry.setCompleted(true);
		if (params.isSync()) {
			params.getLatch().countDown();
		}
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
		RaftEntry entry = new RaftEntry(event, params);

		// 该事件优先级最高并可中断其他事件
		if (event == RaftEvent.FORCE_FOLLOWER) {
			eventQueue.offerFirst(entry);
		} else {
			eventQueue.offerLast(entry);
		}
		if (running && params.isSync()) {
			try {
				params.getLatch().await();
			} catch (InterruptedException e) {
				log.warn("sync event is interrupted, e: {}", e.getMessage());
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

	@Override
	public String println() {
		StringBuilder sb = new StringBuilder();
		sb.append("Running: ").append(running).append("\t");
		sb.append("state: ").append(state).append(System.lineSeparator());
		sb.append("event queue: ").append(eventQueue).append(System.lineSeparator());
		return sb.toString();
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

	@Getter
	@ToString
	private static class RaftEntry {

		private final RaftEvent event;

		private final RaftEventParams params;

		private volatile boolean completed = false;

		public RaftEntry(RaftEvent event, RaftEventParams params) {
			this.event = event;
			this.params = params;
		}

		public void setCompleted(boolean completed) {
			this.completed = completed;
		}
	}
}
