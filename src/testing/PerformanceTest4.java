package testing;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import app_kvEcs.ECS;
import client.KVStore;

public class PerformanceTest4 {
	private Logger logger = Logger.getRootLogger();

	private ECS ecs;

	static {
		try {
			new LogSetup("logs/testing/PerformanceTest" + new Date() + ".log",
					Level.ALL);
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
		ecs.initService(1);
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

	/**
	 * Shut down all servers and disconnect
	 * 
	 * @throws java.lang.Exception
	 */
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

	@Test
	public void OneServerOneClientThousandPut() {
		Thread thread1 = new Thread(new ClientTest());
		Thread thread2 = new Thread(new ClientTest());
		Thread thread3 = new Thread(new ClientTest());
		Thread thread4 = new Thread(new ClientTest());
		Thread thread5 = new Thread(new ClientTest());

		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		thread5.start();

		while (isFinished != 5) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	int isFinished = 0;

	class ClientTest implements Runnable {
		KVStore kvClient;

		@Override
		public void run() {
			String add = ecs.metaData.get("1");
			String[] ss = add.split(":");
			
			String command = "";
			command += "java -jar ms3-test-kvstore.jar ";
			command += ss[0] + " "+ss[1]+"\n";

			Process proc;
			Runtime run = Runtime.getRuntime();
			try {
				System.out.println(command);
				proc = run.exec(command);
			} catch (IOException e) {
				System.out.println("ERROR! Failed to run script");
				logger.error("ERROR! Failed to run script");
				return;
			}
		}

	}
}
