package com.gill.consensus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

class GillConsensusApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	void testCountDownLatch() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		for (int i = 0; i < 3; i++) {
			final int t = i;
			new Thread(() -> {
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				latch.countDown();
				System.out.println(t);
			}).start();
		}
		System.out.println("await");
		latch.await(1000L, TimeUnit.MILLISECONDS);
		System.out.println("wake up");
		LockSupport.park();
	}
}
