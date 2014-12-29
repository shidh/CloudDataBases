package testing;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import client.KVStore;

import app_kvEcs.ECS;

/**
 * Test case: 1 server, 10 clients, sends 100 puts
 * 
 * @author jeff
 * 
 */
public class PerformanceTest5 {
	private Logger logger = Logger.getRootLogger();

	private ECS ecs;

	@Before
	public void setUp() throws Exception {
		ecs = new ECS();
		// Initialize one server and start it
		ecs.initService(11);
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Test
	public void test() {
		String add = ecs.metaData.get("1");
		String[] ss = add.split(":");
		System.out.println("Conencting to " + add);
		KVStore kvStore1 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore2 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore3 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore4 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore5 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore6 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore7 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore8 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore9 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		KVStore kvStore10 = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvStore1.connect();
			kvStore2.connect();
			kvStore3.connect();
			kvStore4.connect();
			kvStore5.connect();
			kvStore6.connect();
			kvStore7.connect();
			kvStore8.connect();
			kvStore9.connect();
			kvStore10.connect();
		} catch (Exception e) {
			logger.error("ERROR! Failed to connect to server " + add);
		}
		long startTime=System.currentTimeMillis();   
		for (int i = 0; i < 100; i++) {
			try {
				kvStore1.put((0+i) + "", (0+i) + "");
				kvStore2.put((100+i) + "", (100+i) + "");
				kvStore3.put((200+i) + "", (200+i) + "");
				kvStore4.put((300+i) + "", (300+i) + "");
				kvStore5.put((400+i) + "", (400+i) + "");
				kvStore6.put((500+i) + "", (500+i) + "");
				kvStore7.put((600+i) + "", (600+i) + "");
				kvStore8.put((700+i) + "", (700+i) + "");
				kvStore9.put((800+i) + "", (800+i) + "");
				kvStore10.put((900+i) + "", (900+i) + "");
			} catch (Exception e) {
				logger.error("ERROR! Cannot put ", e);
			}
		}
		long endTime=System.currentTimeMillis(); 
		System.out.println("One server Ten client one hundred put execution time:\n"+(endTime-startTime)+"ms");
		System.out.println("Test terminated");
	}

	@After
	public void tearDown() throws Exception {
		ecs.shutDown();
		// Wait for latency
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
