package com.gill.consensus.raftplus.state;

import java.util.concurrent.CompletableFuture;

import com.gill.consensus.raftplus.Node;

import lombok.extern.slf4j.Slf4j;

/**
 * Common
 *
 * @author gill
 * @version 2023/09/06
 **/
@Slf4j
public class Common {

	/**
	 * 初始化follower
	 *
	 * @param self
	 *            节点
	 */
	public static void stop(Node self) {
		CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> self.getThreadPools().setClusterPool(null));
		CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> self.getThreadPools().setApiPool(null));
		CompletableFuture.allOf(f1, f2).join();
	}
}
