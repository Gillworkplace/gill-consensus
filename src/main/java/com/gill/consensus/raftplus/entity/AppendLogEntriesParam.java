package com.gill.consensus.raftplus.entity;

import java.util.List;

import com.gill.consensus.raftplus.model.LogEntry;

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * AppendLogEntriesParam
 *
 * @author gill
 * @version 2023/08/18
 **/
@SuperBuilder(builderMethodName = "innerBuilder")
@Getter
@ToString(callSuper = true)
public class AppendLogEntriesParam extends BaseParam {

	private long preLogTerm;

	private int preLogIdx;

	private int commitIdx;

	private List<LogEntry> logs;

	public static AppendLogEntriesParamBuilder<?, ?> builder(int nodeId, long term) {
		return innerBuilder().nodeId(nodeId).term(term);
	}

}
