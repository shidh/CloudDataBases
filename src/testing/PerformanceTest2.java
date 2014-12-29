package testing;

import java.io.IOException;
import java.util.Date;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import app_kvEcs.ECS;
import client.KVStore;

public class PerformanceTest2 {

	private Logger logger = Logger.getRootLogger();

	private KVStore kvClient;
	private ECS ecs;

	static {
		try {
			new LogSetup("logs/testing/PerformanceTest" + new Date() + ".log",
					Level.OFF);
		} catch (IOException e) {
			System.out.println("ERROR! Failed to set up logger");
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		ecs = new ECS();
		// Initialize one server and start it
		ecs.initService(5);
		// Wait for latency
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ecs.start();
		String add = ecs.metaData.get("1");
		String[] ss = add.split(":");
		// Connect to server
		kvClient = new KVStore(ss[0], Integer.valueOf(ss[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
			logger.error("ERROR! Failed to connect to server " + add);
		}
	}

	/**
	 * Shut down all servers and disconnect
	 * 
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		ecs.shutDown();
		try {
			kvClient.disconnect();
		} catch (Exception e) {
			System.out.println("Error! Cannot disconnect");
		}
		// Wait for latency
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Test
	public void FiveServerOneClientThousandPut() {
		long startTime=System.currentTimeMillis();   
		for(int i=0;i<1000;i++){
			try {
				kvClient.put(i+"", i+"");
			} catch (Exception e) {
				logger.error("ERROR! Cannot put ("+i+","+i+")",e);
			}
		}
		long endTime=System.currentTimeMillis(); 
		System.out.println("Five server one client one thousand put execution time:\n"+(endTime-startTime)+"ms");
	}

}
