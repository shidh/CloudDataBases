package client;

import java.io.IOException;
import java.util.Date;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImp;

public class KVStore implements KVCommInterface {
	static int REMOTE_PORT;
	static String REMOTE_IP;
	static int BUF_SIZE = 10000;
	static KVMessage kvmessage;
	static CommunicationLogic communicationLogic = null;

	static Logger logger = Logger.getRootLogger();

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		REMOTE_IP = address;
		REMOTE_PORT = port;
	}

	@Override
	/**
	 * 
	 */
	public void connect() throws Exception {
		try {
			communicationLogic = new CommunicationLogic(REMOTE_IP, REMOTE_PORT);
			communicationLogic.connect();
		} catch (IOException e) {
			throw e;
		}

		String R_message;
		try {
			R_message = communicationLogic.receive();
			logger.info(R_message);
		} catch (IOException e) {
			throw e;
		}
		return;
	}

	public boolean isConnected() throws Exception {
		try {
			return communicationLogic.isConnect();
		} catch (Exception e) {
			throw e;
		}
	}

	@Override
	/**
	 * 
	 */
	public void disconnect() throws Exception {
		try {
			communicationLogic.disconnect();
		} catch (IOException e) {
			throw e;
		}
		return;
	}

	@Override
	/**
	 * 
	 */
	public KVMessage put(String key, String value) throws Exception {
		kvmessage = new KVMessageImp("PUT " + key + " " + value);
		communicationLogic.send(kvmessage.KVMtoStr(kvmessage));
		String R_message;
		try {
			R_message = communicationLogic.receive();
		} catch (IOException e) {
			throw e;
		}
		kvmessage = new KVMessageImp(R_message);
		return kvmessage;

	}

	/**
	 * 
	 */
	public KVMessage get(String key) throws Exception {
		kvmessage = new KVMessageImp("GET " + key);
		communicationLogic.send(kvmessage.KVMtoStr(kvmessage));

		String R_message;
		try {
			R_message = communicationLogic.receive();
		} catch (IOException e) {
			throw e;
		}
		kvmessage = new KVMessageImp(R_message);
		return kvmessage;
	}

	/**
	 * Tears down the active connection to the server and exits the program
	 * execution.
	 * 
	 * @throws IOException
	 */
	public void quit() throws IOException {
		try {
			communicationLogic.disconnect();
		} catch (IOException e) {
			throw e;
		}
		return;
	}

	/**
	 * Sets the logger to the specified log level
	 * 
	 * @param loglevel
	 */
	public void logLevel(String loglevel) {
		if (loglevel.equals("ALL")) {
			logger.setLevel(Level.ALL);
			System.out.println("EchoClient> current log status: ALL");
			logger.info("EchoClient> current log status: ALL");
		} else if (loglevel.equals("DEBUG")) {
			logger.setLevel(Level.DEBUG);
			System.out.println("EchoClient> current log status: DEBUG");
			logger.info("EchoClient> current log status: DEBUG");
		} else if (loglevel.equals("INFO")) {
			logger.setLevel(Level.INFO);
			System.out.println("EchoClient> current log status: INFO");
			logger.info("EchoClient> current log status: INFO");
		} else if (loglevel.equals("WARN")) {
			logger.setLevel(Level.WARN);
			System.out.println("EchoClient> current log status: WARN");
			logger.info("EchoClient> current log status: WARN");
		} else if (loglevel.equals("ERROR")) {
			logger.setLevel(Level.ERROR);
			System.out.println("EchoClient> current log status: ERROR");
			logger.info("EchoClient> current log status: ERROR");
		} else if (loglevel.equals("FATAL")) {
			logger.setLevel(Level.FATAL);
			System.out.println("EchoClient> current log status: FATAL");
			logger.info("EchoClient> current log status: FATAL");
		} else if (loglevel.equals("OFF")) {
			logger.setLevel(Level.OFF);
			System.out.println("EchoClient> current log status: OFF");
			logger.info("EchoClient> current log status: OFF");
		} else {
			System.out.println("Your input level is not existed! ");
			logger.error("Your input level is not existed! ");
		}

		return;
	}

	/**
	 * Prints all the commands to the screen.
	 */
	public String help() {
		return helpTxt;
	}

	public static String helpTxt = "\n"
			+ "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
			+ "command                   |   information\n"
			+ "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
			+ "connect <address> <port>  |   Tries to establish a TCP-connection\n"
			+ "                          |   to the echo server based on the given\n"
			+ "                          |   server address and the port number of\n"
			+ "                          |   the echo service.\n"
			+ "--------------------------|------------------------------------------\n"
			+ "disconnect                |   Tries to disconnect from the connected\n"
			+ "                          |   server.\n"
			+ "--------------------------|------------------------------------------\n"
			+ "put <key> <value>         |   Inserts, updates or deletes a key-value\n"
			+ "                          |   paar.\n"
			+ "--------------------------|------------------------------------------\n"
			+ "get <key>                 |   Retrieves the value for the given key\n"
			+ "                          |   from the storage server.\n"
			+ "--------------------------|------------------------------------------\n"
			+ "logLevel <level>          |   Sets the logger to the specified log \n"
			+ "                          |   level.\n"
			+ "register <key> <type>     |   register a key, type: update or delete or all. \n"
			+ "--------------------------|------------------------------------------\n"
			+ "deregister <key>          |   deregister a key. \n"
			+ "--------------------------|------------------------------------------\n"
			+ "help                      |\n"
			+ "--------------------------|------------------------------------------\n"
			+ "quit                      |   Tears down the active connection to the\n"
			+ "                          |   server and exits the program execution.\n"
			+ "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

	public static void main(String[] args) {
		try {
			new LogSetup("/home/jeff/workspace/DS_MS3/logs/testing/KVStore.log", Level.ALL);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Logger logger=Logger.getRootLogger();
		KVStore kvClient=new KVStore(args[0], Integer.parseInt(args[1]));
		try {
			kvClient.connect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
		// Put one thousand key value pair
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			try {
				kvClient.put(i + "", i + "");
			} catch (Exception e) {
				logger.error("ERROR! Cannot put (" + i + "," + i + ")", e);
			}
		}
		long endTime = System.currentTimeMillis();
		System.out
				.println("One server five clients one thousand put execution time:\n"
						+ (endTime - startTime) + "ms");

		try {
			kvClient.disconnect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
