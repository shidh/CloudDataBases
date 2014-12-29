package testing;

import java.util.Iterator;

import org.junit.Test;

import app_kvEcs.ECS;
import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private ECS ecs;
	
	public void setUp() {
		ecs = new ECS ();
		ecs.initService(1);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		String add = ecs.metaData.get("1");
		String [] ss = add.split(":");
		kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		ecs.shutDown();
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	
	@Test
	public void testPut() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
			System.out.println(response.getStatus());
		} catch (Exception e) {
			ex = e;
		}
		
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() throws Exception {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			System.out.println(response.getStatus());
			
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			System.out.println(response.getStatus());
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
				System.out.println(response.getValue());
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
			System.out.println(response.getStatus());
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	


}
