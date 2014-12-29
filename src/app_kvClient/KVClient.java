package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import logger.LogSetup;

public class KVClient {
	static BufferedReader sysReader;
	static String REMOTE_IP;
	static int REMOTE_PORT;
	static KVStore kvstore;
	static KVMessage kvmessage = null;

	static Logger logger = Logger.getRootLogger();

	// connect
	public static void connect(String IP, int PORT) throws Exception {
		try {
			REMOTE_IP = IP;
			REMOTE_PORT = PORT;
			kvstore = new KVStore(REMOTE_IP, REMOTE_PORT);
			kvstore.connect();
			logger.info("EchoClient> Connection to KVServer " + REMOTE_IP + ":"
					+ REMOTE_PORT + " established!");
		} catch (NumberFormatException e) {
			System.out.println("EchoClient> Illigal port format");
			logger.error("EchoClient> Illigal port format");
		} catch (UnknownHostException e) {
			System.out.println("EchoClient> Uknown host");
			logger.error("EchoClient> Uknown host");
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to connect to KVServer!");
			logger.error("EchoClient> Failed to connect to KVServer!");
		}
	}

	// disconnect
	public static void disconnect() throws Exception {
		try {
			kvstore.disconnect();
			System.out.println("EchoClient> Quit connection!");
			logger.info("EchoClient> Quit connection!");
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to close socketInputStream");
			logger.error("EchoClient> Failed to close socketInputStream");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// put
	public static void put(String key, String value) throws Exception {
		try {
			// If there is a MetaData in kvmessage, reroute according to
			// MetaData.
			if (kvmessage != null && kvmessage.getMetaData() != null) {
				String[] add = kvmessage.getMetaData().get(key).split(":");
				// Should first disconnect from the current server and the reconnect to another one
				kvstore.disconnect();
				connect(add[0], Integer.valueOf(add[1]));
			}
			kvmessage = kvstore.put(key, value);
			if (kvmessage.getStatus().equals(
					StatusType.valueOf("SERVER_STOPPED"))) {
				System.out
						.println("EchoClient> "
								+ "Sorry, now the server is stopped for serving requests.");
				logger.info("EchoClient> "
						+ "Sorry, now the server is stopped for serving requests.");
			} else if (kvmessage.getStatus().equals(
					StatusType.valueOf("SERVER_WRITE_LOCK"))) {
				System.out
						.println("EchoClient> "
								+ "Sorry, now the server is blocked for write requests.");
				logger.info("EchoClient> "
						+ "Sorry, now the server is blocked for write requests.");
			} else if (kvmessage.getStatus().equals(
					StatusType.valueOf("SERVER_NOT_RESPONSIBLE"))) {
				// recursion, now the MetaData has be updated already.
				put(key, value);
			} else {
				System.out.print("EchoClient> " + kvmessage.KVMtoStr(kvmessage)
						+ "\n");
				logger.info("EchoClient> " + kvmessage.KVMtoStr(kvmessage)
						+ "\n");
			}
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to put the <key, value>");
			logger.error("EchoClient> Failed to put the <key, value>");
		}
	}

	// get
	public static void get(String key) throws Exception {
		try {
			// If there is MetaData in kvmessage, reroute according to MetaData.
			if (kvmessage != null && kvmessage.getMetaData() != null) {
				String[] add = kvmessage.getMetaData().get(key).split(":");
				// Should first disconnect from the current server and the reconnect to another one
				kvstore.disconnect();
				connect(add[0], Integer.valueOf(add[1]));
			}
			kvmessage = kvstore.get(key);
			if (kvmessage.getStatus().equals(
					StatusType.valueOf("SERVER_STOPPED"))) {
				System.out
						.println("EchoClient> "
								+ "Sorry, now the server is stopped for serving requests.");
				logger.info("EchoClient> "
						+ "Sorry, now the server is stopped for serving requests.");
			} else if (kvmessage.getStatus().equals(
					StatusType.valueOf("SERVER_NOT_RESPONSIBLE"))) {
				// recursion, now the MetaData has be updated already.
				get(key);
			} else {
				System.out.print("EchoClient> " + kvmessage.KVMtoStr(kvmessage)
						+ "\n");
				logger.info("EchoClient> " + kvmessage.KVMtoStr(kvmessage)
						+ "\n");
			}
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to get the value of key");
			logger.error("EchoClient> Failed to get the value of key");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// loglevel
	public static void loglevel(String loglevel) {
		String logLevel = loglevel;
		kvstore.logLevel(logLevel);
	}

	// help
	public static void help() {
		System.out.print("EchoClient> ");
		logger.info("EchoClient> ");
		System.out.println(KVStore.helpTxt);
		logger.info(KVStore.helpTxt);
	}

	// quit
	public static void quit() throws IOException {
		if(kvstore!=null){
			kvstore.quit();
		}
		System.out.println("EchoClient> Application exit!");
		logger.info("EchoClient> Application exit!");
	}

	/**
	 * Entrance of the application
	 * 
	 * @param args
	 *            parameters from command line
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Application logic
		new LogSetup("logs/client/client.log", Level.OFF);

		sysReader = new BufferedReader(new InputStreamReader(System.in));
		String str = null;
		while (true) {
			System.out.print("EchoClient> ");
			logger.info("EchoClient> ");

			// Read from system input
			try {
				str = sysReader.readLine();
			} catch (IOException e) {
				System.out
						.println("EchoClient> Failed to read from system input");
				logger.error("EchoClient> Failed to read from system input");
			}
			if (!str.trim().equals("")) {
				// Get command
				String[] token = str.split(" ");
				String cmd = token[0];
				// connect
				if (cmd.equals("connect")) {
					if (kvstore == null || kvstore.isConnected() == false) {
						int length = token.length;
						if (length == 3) {
							// Get remote server address and port
							connect(token[1], Integer.parseInt(token[2]));
							System.out
									.println("EchoClient> Connection to KVServer established!");
							// Wrong format
						} else {
							System.out
									.println("EchoClient> Connect command should be as follow:\nconnect <address> <port>");
							logger.error("EchoClient> Connect command should be as follow:\nconnect <address> <port>");
						}
					} else {
						System.out
								.println("EchoClient> You have connected to a server already!");
						logger.error("EchoClient> You have connected to a server already!");
					}

				}
				// disconnect
				else if (cmd.equals("disconnect")) {
					if (kvstore.isConnected() == true) {
						disconnect();
					} else {
						System.out
								.println("EchoClient> You are not connected to a server!");
						logger.error("EchoClient> You are not connected to a server!");
					}
				}
				// put
				else if (cmd.equals("put")) {
					if (kvstore.isConnected() == true) {
						put(token[1], token[2]);
					} else {
						System.out
								.println("EchoClient> You are not connected to a server!");
						logger.error("EchoClient> You are not connected to a server!");
					}

				}
				// get
				else if (cmd.equals("get")) {
					if (kvstore.isConnected() == true) {
						get(token[1]);
					} else {
						System.out
								.println("EchoClient> You are not connected to a server!");
						logger.error("EchoClient> You are not connected to a server!");
					}
				}
				// logLevel
				else if (cmd.equals("logLevel")) {
					int length = token.length;
					if (length == 2) { // Get logLevel String
						loglevel(token[1]);
					}
					// Wrong format
					else {
						System.out
								.println("logLevel command should be as follow:\nlogLevel <Level> ");
						logger.error("logLevel command should be as follow:\nlogLevel <Level> ");
					}
				}
				// help
				else if (cmd.equals("help")) {
					help();
				}
				// quit
				else if (cmd.equals("quit")) {
					if (kvstore!=null && kvstore.isConnected() == true) {
						quit();
					} else {
						System.out
								.println("EchoClient> You are not connected to a server!");
						logger.error("EchoClient> You are not connected to a server!");
					}
					return;
				}
				// other inputs
				else {
					System.out
							.println("EchoClient> Incorrect command, please try again or input 'help'");
					logger.error("EchoClient> Incorrect command, please try again or input 'help'");
				}
			}
		}
	}

}
