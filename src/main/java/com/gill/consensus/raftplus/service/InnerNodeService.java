package com.gill.consensus.raftplus.service;

import com.gill.consensus.raftplus.common.Utils;
import com.gill.consensus.raftplus.entity.AppendLogEntriesParam;
import com.gill.consensus.raftplus.entity.AppendLogReply;
import com.gill.consensus.raftplus.entity.PreVoteParam;
import com.gill.consensus.raftplus.entity.ReplicateSnapshotParam;
import com.gill.consensus.raftplus.entity.Reply;
import com.gill.consensus.raftplus.entity.RequestVoteParam;

/**
 * NodeService
 *
 * @author gill
 * @version 2023/08/18
 **/
public interface InnerNodeService extends NodeService {

	/**
	 * 获取id
	 *
	 * @return id
	 */
	int getID();

	/**
	 * raft 预投票（防止term膨胀）
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	default Reply preVote(PreVoteParam param) {
		if (!ready()) {
			return new Reply(false, -1);
		}
		return Utils.cost(() -> doPreVote(param), "pre-vote");
	}

	/**
	 * raft 预投票（防止term膨胀）
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	Reply doPreVote(PreVoteParam param);

	/**
	 * raft 投票
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	default Reply requestVote(RequestVoteParam param) {
		if (!ready()) {
			return new Reply(false, -1);
		}
		return Utils.cost(() -> doRequestVote(param), "request-vote");
	}

	/**
	 * raft 投票
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	Reply doRequestVote(RequestVoteParam param);

	/**
	 * ping 和 日志同步
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	default AppendLogReply appendLogEntries(AppendLogEntriesParam param) {
		if (!ready()) {
			return new AppendLogReply(false, -1);
		}
		return Utils.cost(() -> doAppendLogEntries(param), "append-log-entries");
	}

	/**
	 * ping 和 日志同步
	 *
	 * @param param
	 *            param
	 * @return Reply
	 */
	AppendLogReply doAppendLogEntries(AppendLogEntriesParam param);

	/**
	 * 同步快照数据
	 * 
	 * @param param
	 *            参数
	 * @return 响应
	 */
	default Reply replicateSnapshot(ReplicateSnapshotParam param) {
		if (!ready()) {
			return new AppendLogReply(false, -1);
		}
		return Utils.cost(() -> doReplicateSnapshot(param), "replicate-snapshot");
	}

	/**
	 * 同步快照数据
	 *
	 * @param param
	 *            参数
	 * @return 响应
	 */
	Reply doReplicateSnapshot(ReplicateSnapshotParam param);

	/**
	 * toString
	 *
	 * @return 内容
	 */
	String toString();
}
