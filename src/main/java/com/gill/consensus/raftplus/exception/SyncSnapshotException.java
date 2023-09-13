package com.gill.consensus.raftplus.exception;

/**
 * SyncSnapshotException
 *
 * @author gill
 * @version 2023/09/12
 **/
public class SyncSnapshotException extends Exception {

	public SyncSnapshotException() {
		super();
	}

	public SyncSnapshotException(String message) {
		super(message);
	}

	public SyncSnapshotException(String message, Throwable cause) {
		super(message, cause);
	}

	public SyncSnapshotException(Throwable cause) {
		super(cause);
	}

	protected SyncSnapshotException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
