package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import client.CommunicationLogic;
import common.messages.KVMessage.StatusType;
import common.messages.MetaData;

public class Listen implements Runnable {

	private int BUF_SIZE = 1024;

	static Socket socket = null;
	InputStream inputStream;
	OutputStream outputStream;

	Logger logger = Logger.getRootLogger();

	public Listen(Socket socket2) {
		try {
			socket = socket2;
			System.out.println("ACCEPT new client connected ["
					+ socket.getRemoteSocketAddress() + "]");
			logger.info("ACCEPT new client connected ["
					+ socket.getRemoteSocketAddress() + "]");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	@Override
	public void run() {
		try {
			if (socket != null) {
				try {
					inputStream = socket.getInputStream();
				} catch (IOException e1) {
					logger.error("Error getting input stream");
				}
				try {
					outputStream = socket.getOutputStream();
					outputStream
							.write("EchoClient> Connection to KVServer established!"
									.getBytes());
					outputStream.flush();
				} catch (IOException e1) {
					logger.error("error sending greading message");
				}

				// Echo messages to client
				while (socket.isConnected()) {
					try {
						String rec_message = receive();
						logger.info("receive successfully " + rec_message);
						// When the client close the socket, lots of empty
						// strings
						// will be received. In this case, we just skip it.
						if (rec_message.equals("")) {
							return;
						}

						System.out.println("RECEIVE '" + rec_message
								+ "' FROM [" + socket.getRemoteSocketAddress()
								+ "]");
						logger.info("RECEIVE '" + rec_message + "' FROM ["
								+ socket.getRemoteSocketAddress() + "]");
						String[] ss = rec_message.split(" ");
						if (ss[0].equals("PUT")) {
							put(ss);

							MetaData metaData = DataSingleton.getInstance()
									.getMetaData();
							String serverAdd = metaData.get(ss[1]);
							String split[] = serverAdd.split(":");
							String IP = split[0];
							String Port = split[1];
							String ip = socket.getLocalAddress().toString()
									.replace("/", "");
							// If this is the right server
							if (IP.equals(ip)
									&& Port.equals(socket.getLocalPort() + "")) {
								logger.info("begin backup");
								backUp(ss[1], ss[2]);
							}

						} else if (ss[0].equals("GET")) {
							get(ss);
						} else if (ss[0].equals("ECS")) {
							// ECS messages
							String operation = ss[1];
							if (operation.equals("initService")) {
								initService(rec_message);
							} else if (operation.equals("start")) {
								start();
							} else if (operation.equals("stop")) {
								stop();
							} else if (operation.equals("shutDown")) {
								shutDown();
							} else if (operation.equals("addNode")) {
								addNode(rec_message);
							} else if (operation.equals("updateMetaData")) {
								updateMetaData(rec_message);
							} else if (operation.equals("moveDataTo")) {
								moveDataTo(ss[2]);
							} else if (operation.equals("removeNode")) {
								removeNode(ss);
							} else if (operation.equals("backup")) {
								backupALL();
							}
						} else if (ss[0].equals("SERVER")) {
							// Handle server messages
							String operation = ss[1];
							if (operation.equals("backup")) {
								// TODO get backup message and implement backup
								// method
								doBackUp(ss[2], ss[3]);
							}
						}
					} catch (IOException e) {
						logger.error("Failed to receive from server");
						return;
					}
				}
			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	//
	private void addNode(String rec_message) throws IOException {
		try {
			String split[] = rec_message.split("\n");
			logger.info("[META-DATA] " + split[1]);
			// Construct meta data and save it into data
			// singleton
			DataSingleton.getInstance().setMetaData(new MetaData(split[1]));
			// Send back ack
			send("ack");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void removeNode(String[] ss) throws IOException {
		try {
			// Set to SERVER_WRITE_LOCK
			// DataSingleton.getInstance().setStatus(StatusType.SERVER_WRITE_LOCK);
			// Get next server
			String split[] = ss[3].split(":");
			String nextServerIP = split[0];
			String nextServerPort = split[1];
			// Traverse data
			Map data = DataSingleton.getInstance().getMap();
			Iterator<Map.Entry<String, String>> it = data.entrySet().iterator();
			while (it.hasNext()) {
				// Get each entry of data
				Map.Entry<String, String> entry = it.next();
				String key = entry.getKey();
				String value = entry.getValue();
				// communicate next server and move data to it
				CommunicationLogic communicateMove = new CommunicationLogic(
						nextServerIP, Integer.parseInt(nextServerPort));
				// Connect
				communicateMove.connect();
				// Receive
				String rec_msg = communicateMove.receive();
				System.out.println(rec_msg);
				logger.info(rec_msg);
				// Send
				communicateMove.send("PUT " + key + " " + value);
				// Receive
				rec_msg = communicateMove.receive();
				System.out.println(rec_msg);
				// Disconnect
				communicateMove.disconnect();
			}
			// Set to undo write lock
			// DataSingleton.getInstance().setStatus(StatusType.SERVER_WRITE_LOCK);
			shutDown();
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	// When adding a node, call backupALL(key, bull) on the next node to delete
	// String nextServer = metaData.get(ip + ":" + port);
	private void moveDataTo(String previousServer) throws IOException {
		try {
			// Set to SERVER_WRITE_LOCK
			// DataSingleton.getInstance().setStatus(StatusType.SERVER_WRITE_LOCK);
			// Get meta data
			MetaData metaData = DataSingleton.getInstance().getMetaData();
			// Get all data
			// Traverse data
			Map<String, String> data = DataSingleton.getInstance().getMap();
			ArrayList<String> delList = new ArrayList<String>();
			// while (it.hasNext()) {
			for (Entry<String, String> entry : data.entrySet()) {
				// Get each entry of data
				String key = entry.getKey();
				String value = entry.getValue();
				// Check where the data should be placed
				String shouldBeServer = metaData.get(key);
				// Send data to the target server
				String split[] = shouldBeServer.split(":");
				String targetIP = split[0];
				String targetPort = split[1];
				logger.info(key + "shouldBeServer" + shouldBeServer);
				logger.info(key + "previousServer" + previousServer);
				// If not this server then send it to target
				if (!targetPort.equals(socket.getLocalPort() + "")) {
					// remove the unnecessary backup data on the next two nodes
					backUp(key, "null");
					// DataSingleton.getInstance().getMap().remove(key);
					if (!shouldBeServer.equals(previousServer)) {
						delList.add(key);
					}
					if (shouldBeServer.equals(previousServer)) {
						CommunicationLogic communicateMove = new CommunicationLogic(
								targetIP, Integer.parseInt(targetPort));
						// Connect
						communicateMove.connect();
						// Receive
						String rec_msg = communicateMove.receive();
						System.out.println(rec_msg);
						logger.info(rec_msg);
						// Send
						communicateMove.send("PUT " + key + " " + value);
						// Receive
						rec_msg = communicateMove.receive();
						System.out.println(rec_msg);
						logger.info(rec_msg);
						// Disconnect
						communicateMove.disconnect();
					}

				}
			}
			for (int i = 0; i < delList.size(); i++) {
				DataSingleton.getInstance().getMap().remove(delList.get(i));
			}
			// Set to undo write lock
			// DataSingleton.getInstance().setStatus(StatusType.SERVER_WRITE_LOCK);
			send("ack");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void updateMetaData(String rec_message) throws IOException {
		try {
			String split[] = rec_message.split("\n");
			logger.info("[UPDATE META-DATA] " + split[1]);
			// Construct meta data and save it into data
			// singleton
			DataSingleton.getInstance().setMetaData(new MetaData(split[1]));
			// Send back ack
			send("ack");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void shutDown() throws IOException {
		try {
			send("ack");
			System.exit(0);
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void stop() throws IOException {
		try {
			DataSingleton.getInstance().setStatus(StatusType.SERVER_STOPPED);
			send("ack");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void start() throws IOException {
		try {
			DataSingleton.getInstance().setStatus(StatusType.SERVER_ACTIVE);
			send("ack");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void initService(String rec_message) throws IOException {
		try {
			String split[] = rec_message.split("\n");
			logger.info("[META-DATA] " + split[1]);
			// Construct meta data and save it into data
			// singleton
			DataSingleton.getInstance().setMetaData(new MetaData(split[1]));
			stop();
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	// to change
	private void get(String[] ss) throws IOException {
		try {
			// Check status
			StatusType status = DataSingleton.getInstance().getStatus();
			if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
				send("SERVER_STOPPED");
			} else {
				MetaData metaData = DataSingleton.getInstance().getMetaData();
				String serverAdd = metaData.get(ss[1]);
				String split[] = serverAdd.split(":");
				String IP = split[0];
				String Port = split[1];
				String ip = socket.getLocalAddress().toString()
						.replace("/", "");
				// If this is the right server
				if (IP.equals(ip) && Port.equals(socket.getLocalPort() + "")) {
					if (DataSingleton.getInstance().containsKey(ss[1]) == false) {
						String send_message = "GET_ERROR " + ss[1];
						send(send_message);
					} else {
						String send_message = "GET_SUCCESS " + ss[1] + " "
								+ DataSingleton.getInstance().get(ss[1]);
						send(send_message);
					}
				}
				// If this is not the right server, response with
				// NOT RESPONSIBLE and send back the newest meta
				else {
					if (DataSingleton.getInstance().containsKey(ss[1]) == false) {
						send("SERVER_NOT_RESPONSIBLE " + metaData.toString());
					} else {
						String send_message = "GET_SUCCESS " + ss[1] + " "
								+ DataSingleton.getInstance().get(ss[1]);
						send(send_message);
					}
					
				}
			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private void put(String[] ss) throws IOException {
		try {
			// Check status
			StatusType status = DataSingleton.getInstance().getStatus();
			if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
				send("SERVER_STOPPED");
			} else if (status.equals(StatusType.valueOf("SERVER_WRITE_LOCK"))) {
				send("SERVER_WRITE_LOCK");
			} else {
				MetaData metaData = DataSingleton.getInstance().getMetaData();
				String serverAdd = metaData.get(ss[1]);
				String split[] = serverAdd.split(":");
				String IP = split[0];
				String Port = split[1];
				String ip = socket.getLocalAddress().toString()
						.replace("/", "");
				// If this is the right server
				if (IP.equals(ip) && Port.equals(socket.getLocalPort() + "")) {
					if (DataSingleton.getInstance().containsKey(ss[1]) == false) {
						if (ss[2].equals("null")) {
							String send_message = "DELETE_ERROR " + ss[1];
							send(send_message);
						} else {
							DataSingleton.getInstance().put(ss[1], ss[2]);
							String send_message = "PUT_SUCCESS " + ss[1] + " "
									+ ss[2];
							send(send_message);

						}
					} else {
						if (ss[2].equals("null")) {
							String send_message = "DELETE_SUCCESS " + ss[1]
									+ " "
									+ DataSingleton.getInstance().get(ss[1]);
							DataSingleton.getInstance().remove(ss[1]);
							send(send_message);
						} else {
							DataSingleton.getInstance().put(ss[1], ss[2]);
							String send_message = "PUT_UPDATE " + ss[1] + " "
									+ ss[2];
							send(send_message);
						}
					}
				}
				// If this is not the right server, response with
				// NOT RESPONSIBLE and send back the newest meta
				else {
					send("SERVER_NOT_RESPONSIBLE " + metaData.toString());
				}

			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	private String receive() throws IOException {
		String rec_message = null;
		try {
			int length;
			byte r[] = new byte[BUF_SIZE];
			length = inputStream.read(r);
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < length; i++) {
				sb.append((char) (r[i]));
			}
			rec_message = sb.toString();
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
		return rec_message;

	}

	/**
	 * send data to client
	 * 
	 * @param send_message
	 * @throws IOException
	 */
	private void send(String send_message) throws IOException {
		try {
			outputStream.write(send_message.getBytes());
			outputStream.flush();
			System.out.println("SEND '" + send_message + "'");
			logger.info("SEND '" + send_message + "' TO ["
					+ socket.getRemoteSocketAddress() + "]");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	/**
	 * For coordinator node Send backup command to two replica node instances
	 * with the coordinator node data
	 * 
	 * @param key
	 * @param value
	 */
	public void backUp(String key, String value) {
		try {
			MetaData metaData = DataSingleton.getInstance().getMetaData();
			ArrayList<String> replicaList = new ArrayList<String>();

			// get the next two servers
			String ip = socket.getLocalAddress().toString().replace("/", "");
			;
			int port = socket.getLocalPort();
			logger.info("my ip port " + ip + " " + port);

			StringBuilder sb = new StringBuilder();
			logger.info("there are " + metaData.getCircle().entrySet().size()
					+ " nodes");
			for (Entry<String, String> entry : metaData.getCircle().entrySet()) {
				sb.append("Key: " + entry.getKey() + ". Value: "
						+ entry.getValue() + " | ");
			}
			logger.info(sb.toString());
			String nextServer1 = metaData.getNext(ip + ":" + port);
			String split1[] = nextServer1.split(":");
			String nextServerIP = split1[0];
			String nextServerPort = split1[1];
			logger.info("server1 " + nextServer1);

			replicaList.add(nextServer1);

			String nextServer2 = metaData.getNext(nextServerIP + ":"
					+ nextServerPort);
			replicaList.add(nextServer2);
			logger.info("server2 " + nextServer2);
			// "Put" data to the two replica servers
			for (String replica : replicaList) {
				String split[] = replica.split(":");

				// Connect to server
				CommunicationLogic communication = new CommunicationLogic(
						split[0], Integer.parseInt(split[1]));
				try {
					communication.connect();
					String rec_msg = communication.receive();
					logger.info("receive message from replica server" + rec_msg);

					// backup data to server
					logger.info("SERVER " + "backup " + key + " " + value);
					communication.send("SERVER " + "backup " + key + " "
							+ value);
					// Receive
					rec_msg = communication.receive();
					logger.info("receive message from replica server" + rec_msg);

					// Receive ack
					if (rec_msg.split(" ")[0].equals("BACKUP_SUCCESS")) {
						logger.info("backup data " + rec_msg.split(" ")[1]
								+ ":" + rec_msg.split(" ")[2] + " successfully");
					} else if (rec_msg.split(" ")[0].equals("BACKUP_UPDATE")) {
						logger.info("update data " + rec_msg.split(" ")[1]
								+ ":" + rec_msg.split(" ")[2] + " successfully");
					}
					communication.disconnect();
				} catch (IOException e) {
					System.out.println("ERROR! Cannot connect to server");
					logger.error("ERROR! Cannot connect to server");
				} catch (Exception e) {
					System.out.println("ERROR!");
					logger.error("ERROR!");
				}

			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	/**
	 * For coordinator node backup all data
	 * 
	 */
	private void backupALL() {
		try {
			for (Entry<String, String> entry : DataSingleton.getInstance()
					.getMap().entrySet()) {
				MetaData metaData = DataSingleton.getInstance().getMetaData();
				String serverAdd = metaData.get(entry.getKey());
				logger.info("keys for backup" + entry.getKey());
				String split[] = serverAdd.split(":");
				String IP = split[0];
				String Port = split[1];
				String ip = socket.getLocalAddress().toString()
						.replace("/", "");
				// If this is the right server
				if (IP.equals(ip) && Port.equals(socket.getLocalPort() + "")) {
					backUp(entry.getKey(), entry.getValue());
				}
			}

			try {
				send("ack");
			} catch (IOException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				logger.error(sw.toString());
			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

	
	/**
	 * for replica node replicas execute the backup(PUT Operation) and ack
	 * @param key
	 * @param value
	 * @throws IOException
	 * 
	 * send message back to the coordinator
	 */
	private void doBackUp(String key, String value) throws IOException {
		try {
			// Check status
			StatusType status = DataSingleton.getInstance().getStatus();
			if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
				send("SERVER_STOPPED");
				logger.error("backup failed");
			} else if (status.equals(StatusType.valueOf("SERVER_WRITE_LOCK"))) {
				send("SERVER_WRITE_LOCK");
				logger.error("backup failed");
			} else {
				// If the replica server doesn't hold the key
				if (!DataSingleton.getInstance().containsKey(key)) {
					if (value.equals("null")) {
						String send_message = "BACKUP_NA " + key + " " + value;
						send(send_message);
						return;
					}
					DataSingleton.getInstance().put(key, value);
					String send_message = "BACKUP_SUCCESS " + key + " " + value;
					send(send_message);

					// check data is backup or not
					for (Entry<String, String> entry : DataSingleton
							.getInstance().getMap().entrySet()) {
						logger.info("Data: " + entry.getKey() + ":"
								+ entry.getValue());
					}

				} else {
					String send_message;
					if (value.equals("null")) {
						MetaData metaData = DataSingleton.getInstance()
								.getMetaData();
						String serverAdd = metaData.get(key);
						logger.info("keys for backup" + key);
						String split[] = serverAdd.split(":");
						String IP = split[0];
						String Port = split[1];
						String ip = socket.getLocalAddress().toString()
								.replace("/", "");
						// If this is the right server
						if (IP.equals(ip)
								&& Port.equals(socket.getLocalPort() + "")) {
							send_message = "BACKUP_REFUSE " + key + " " + value;
						} else {
							DataSingleton.getInstance().remove(key);
							send_message = "BACKUP_DELETE " + key + " " + value;
						}
					} else {
						DataSingleton.getInstance().put(key, value);
						send_message = "BACKUP_UPDATE " + key + " " + value;
					}

					send(send_message);
					// check data is backup or not
					for (Entry<String, String> entry : DataSingleton
							.getInstance().getMap().entrySet()) {
						logger.info("Data: " + entry.getKey() + ":"
								+ entry.getValue());
					}

				}
			}
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

}
