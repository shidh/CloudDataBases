package testing;

import java.net.UnknownHostException;

import app_kvEcs.ECS;
import client.KVStore;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	private KVStore kvClient;
	private ECS ecs;
	
	public void testConnectionSuccess() {
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
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
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
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		ecs = new ECS ();
		ecs.initService(1);
		ecs.start();
		String add = ecs.metaData.get("1");
		String [] ss = add.split(":");
		
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", Integer.valueOf(ss[1]));
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		ecs.shutDown();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		ecs = new ECS ();
		ecs.initService(1);
		ecs.start();
		String add = ecs.metaData.get("1");
		String [] ss = add.split(":");
		
		Exception ex = null;
		KVStore kvClient = new KVStore(ss[0], 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		ecs.shutDown();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

