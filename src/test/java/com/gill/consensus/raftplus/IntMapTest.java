package com.gill.consensus.raftplus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.boot.test.context.SpringBootTest;

import com.gill.consensus.BaseTest;
import com.gill.consensus.raftplus.mock.MockIntMapServer;

import cn.hutool.core.util.RandomUtil;

/**
 * IntMapTest
 *
 * @author gill
 * @version 2023/09/18
 **/
@SpringBootTest
public class IntMapTest extends BaseTest {

	private static List<MockIntMapServer> init(int num) {
		List<MockIntMapServer> servers = new ArrayList<>();
		int offset = RandomUtil.randomInt(1000) * 100;
		for (int i = 0; i < num; i++) {
			servers.add(new MockIntMapServer(offset + i));
		}
		System.out.println("offset: " + offset);
		return servers;
	}

	private static MockIntMapServer findLeader(List<MockIntMapServer> servers) {
		Optional<MockIntMapServer> leaderOpt = servers.stream().filter(MockIntMapServer::isLeader).findFirst();
		if (!leaderOpt.isPresent()) {
			Assertions.fail("not find leader");
		}
		return leaderOpt.get();
	}

	private static void waitUtilStable(List<MockIntMapServer> servers) {
		while (true) {
			Optional<MockIntMapServer> leader = servers.stream().filter(MockIntMapServer::isLeader).findFirst();
			if (leader.isPresent() && leader.get().getNode().isStable()) {
				break;
			}
			sleep(10);
		}
	}

	private static void stopServers(List<MockIntMapServer> servers) {
		for (MockIntMapServer server : servers) {
			server.stop();
		}
	}

	private static void assertCluster(List<MockIntMapServer> servers) {
		int leaderCnt = 0;
		int followerCnt = 0;
		int availableCnt = 0;
		for (MockIntMapServer server : servers) {
			if (server.isLeader()) {
				leaderCnt++;
			}
			if (server.isFollower()) {
				followerCnt++;
			}
			if (server.isUp()) {
				availableCnt++;
			}
		}
		try {
			Assertions.assertEquals(1, leaderCnt, "leader 数目异常");
			Assertions.assertEquals(availableCnt - 1, followerCnt, "follower 数目异常");
		} catch (Throwable e) {
			System.out.println("============ AFTER EXCEPTION ===============");
			stopServers(servers);
			throw e;
		}
	}

	private List<MockIntMapServer> nodesInit(int num) {
		List<MockIntMapServer> servers = init(num);
		List<Node> nodes = servers.stream().map(MockIntMapServer::getNode).collect(Collectors.toList());
		for (MockIntMapServer server : servers) {
			server.start(nodes);
		}
		waitUtilStable(servers);
		assertCluster(servers);
		return servers;
	}

	@RepeatedTest(30)
	public void testPutGetCommand_Leader() {
		List<MockIntMapServer> servers = nodesInit(5);
		MockIntMapServer leader = findLeader(servers);
		leader.set("test", 123);
		System.out.println("============ TEST FINISHED ===============");
		Assertions.assertEquals(123, leader.get("test"));
		stopServers(servers);
	}
}
