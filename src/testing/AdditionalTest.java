package testing;

import org.junit.Test;

import app_kvEcs.ECS;
import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	private ECS ecs;

	@Test
	public void testServer_stopped() {
		ecs = new ECS();
		ecs.initService(1);
		String add = ecs.metaData.get("1");
		String[] ss = add.split(":");
		kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
		}

		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		ecs.shutDown();
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertTrue(ex == null
				&& response.getStatus() == StatusType.SERVER_STOPPED);
	}

	public void testMetadataUpdate() {
		ecs = new ECS();
		ecs.initService(2);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		String add = ecs.metaData.get("1");
		String[] ss = add.split(":");
		kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
		}

		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		ecs.shutDown();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}

		if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE)
			assertTrue(ex == null && response.getMetaData() != null);
	}

	@Test
	public void testConsistentHash() {
		ecs = new ECS();
		ecs.initService(2);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		String add = ecs.metaData.get("1");
		String[] ss = add.split(":");
		kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
		}

		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		ecs.shutDown();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}

		if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			String newAd = response.getMetaData().get(key);
			String[] AD = newAd.split(":");
			assertTrue(ex == null
					&& Integer.valueOf(AD[1]) != Integer.valueOf(ss[1]));
		}
	}

	@Test
	public void testAddNode() {
		ecs = new ECS();
		ecs.initService(2);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		int Prior_Number_of_Nodes = ecs.metaData.getCircle().keySet().size();
		ecs.addNode();
		int Post_Number_of_Nodes = ecs.metaData.getCircle().keySet().size();
		ecs.shutDown();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}

		assertTrue(Post_Number_of_Nodes == Prior_Number_of_Nodes + 1);
	}

	@Test
	public void testRemoveNode() {
		ecs = new ECS();
		ecs.initService(3);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		int Prior_Number_of_Nodes = ecs.metaData.getCircle().keySet().size();
		ecs.removeNode();
		int Post_Number_of_Nodes = ecs.metaData.getCircle().keySet().size();
		ecs.shutDown();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}

		assertTrue(Post_Number_of_Nodes == Prior_Number_of_Nodes - 1);
	}

}