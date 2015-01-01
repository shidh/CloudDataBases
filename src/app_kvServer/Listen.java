package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.Data;

import logger.LogSetup;

import org.apache.log4j.Level;
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
		socket = socket2;
		System.out.println("ACCEPT new client connected ["
				+ socket.getRemoteSocketAddress() + "]");
		logger.info("ACCEPT new client connected ["
				+ socket.getRemoteSocketAddress() + "]");
	}

	@Override
	public void run() {
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
					// When the client close the socket, lots of empty strings
					// will be received. In this case, we just skip it.
					if (rec_message.equals("")) {
						return;
					}

					System.out.println("RECEIVE '" + rec_message + "' FROM ["
							+ socket.getRemoteSocketAddress() + "]");
					logger.info("RECEIVE '" + rec_message + "' FROM ["
							+ socket.getRemoteSocketAddress() + "]");
					String[] ss = rec_message.split(" ");
					if (ss[0].equals("PUT")) {
						put(ss);
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
							moveDataTo();
						} else if (operation.equals("removeNode")) {
							removeNode(ss);
						}
					} else if (ss[0].equals("SERVER")) {
						// Handle server messages
						String operation = ss[1];
						if (operation.equals("backUp")) {
							//TODO get backup message and implement backup method 
							ackBackUp(ss);
						}
					}
				} catch (IOException e) {
					logger.error("Failed to receive from server");
					return;
				}
			}
		}
	}
	
	private void removeNode(String[] ss) throws IOException {
		// Set to SERVER_WRITE_LOCK
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_WRITE_LOCK);
		// Get next server
		String split[] = ss[3].split(":");
		String nextServerIP = split[0];
		String nextServerPort = split[1];
		// Traverse data
		Map data = DataSingleton.getInstance().getMap();
		Iterator<Map.Entry<String, String>> it = data
				.entrySet().iterator();
		while (it.hasNext()) {
			// Get each entry of data
			Map.Entry<String, String> entry = it.next();
			String key = entry.getKey();
			String value = entry.getValue();
			// communicate next server and move data to it
			CommunicationLogic communicateMove = new CommunicationLogic(
					nextServerIP,
					Integer.parseInt(nextServerPort));
			// Connect
			communicateMove.connect();
			// Receive
			String rec_msg = communicateMove.receive();
			System.out.println(rec_msg);
			logger.info(rec_msg);
			// Send
			communicateMove
					.send("PUT " + key + " " + value);
			// Receive
			rec_msg = communicateMove.receive();
			System.out.println(rec_msg);
			// Disconnect
			communicateMove.disconnect();
		}
		// Set to undo write lock
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_WRITE_LOCK);
		shutDown();
	}

	private void moveDataTo() throws IOException {
		// Set to SERVER_WRITE_LOCK
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_WRITE_LOCK);
		// Get meta data
		MetaData metaData = DataSingleton.getInstance()
				.getMetaData();
		// Get all data
		// Traverse data
		Map data = DataSingleton.getInstance().getMap();
		Iterator<Map.Entry<String, String>> it = data
				.entrySet().iterator();
		while (it.hasNext()) {
			// Get each entry of data
			Map.Entry<String, String> entry = it.next();
			String key = entry.getKey();
			String value = entry.getValue();
			// Check where the data should be placed
			String shouldBeServer = metaData.get(key);
			// Send data to the target server
			String split[] = shouldBeServer.split(":");
			String targetIP = split[0];
			String targetPort = split[1];
			// If not this server then send it to target
			if (!targetPort.equals(socket.getLocalPort()
					+ "")) {
				CommunicationLogic communicateMove = new CommunicationLogic(
						targetIP,
						Integer.parseInt(targetPort));
				// Connect
				communicateMove.connect();
				// Receive
				String rec_msg = communicateMove.receive();
				System.out.println(rec_msg);
				logger.info(rec_msg);
				// Send
				communicateMove.send("PUT " + key + " "
						+ value);
				// Receive
				rec_msg = communicateMove.receive();
				System.out.println(rec_msg);
				// Disconnect
				communicateMove.disconnect();
				// Delete local data
				it.remove();
			}
		}
		// Set to undo write lock
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_WRITE_LOCK);
		send("ack");
	}

	private void updateMetaData(String rec_message) throws IOException {
		String split[] = rec_message.split("\n");
		logger.info("[UPDATE META-DATA] " + split[1]);
		// Construct meta data and save it into data
		// singleton
		DataSingleton.getInstance().setMetaData(
				new MetaData(split[1]));
		// Send back ack
		send("ack");
	}

	private void addNode(String rec_message) throws IOException {
		String split[] = rec_message.split("\n");
		logger.info("[META-DATA] " + split[1]);
		// Construct meta data and save it into data
		// singleton
		DataSingleton.getInstance().setMetaData(
				new MetaData(split[1]));
		// Send back ack
		send("ack");
	}

	private void shutDown() throws IOException {
		send("ack");
		System.exit(0);
	}

	private void stop() throws IOException {
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_STOPPED);
		send("ack");
	}

	private void start() throws IOException {
		DataSingleton.getInstance().setStatus(
				StatusType.SERVER_ACTIVE);
		send("ack");
	}

	private void initService(String rec_message) throws IOException {
		String split[] = rec_message.split("\n");
		logger.info("[META-DATA] " + split[1]);
		// Construct meta data and save it into data
		// singleton
		DataSingleton.getInstance().setMetaData(
				new MetaData(split[1]));
		stop();
	}

	private void get(String[] ss) throws IOException {
		// Check status
		StatusType status = DataSingleton.getInstance()
				.getStatus();
		if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
			send("SERVER_STOPPED");
		} else {
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
				if (DataSingleton.getInstance().containsKey(
						ss[1]) == false) {
					String send_message = "GET_ERROR " + ss[1];
					send(send_message);
				} else {
					String send_message = "GET_SUCCESS "
							+ ss[1]
							+ " "
							+ DataSingleton.getInstance().get(
									ss[1]);
					send(send_message);
				}
			}
			// If this is not the right server, response with
			// NOT RESPONSIBLE and send back the newest meta
			else {
				send("SERVER_NOT_RESPONSIBLE "
						+ metaData.toString());
			}
		}
	}

	private void put(String[] ss) throws IOException {
		// Check status
		StatusType status = DataSingleton.getInstance()
				.getStatus();
		if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
			send("SERVER_STOPPED");
		} else if (status.equals(StatusType
				.valueOf("SERVER_WRITE_LOCK"))) {
			send("SERVER_WRITE_LOCK");
		} else {
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
				if (DataSingleton.getInstance().containsKey(
						ss[1]) == false) {
					if (ss[2].equals("null")) {
						String send_message = "DELETE_ERROR "
								+ ss[1];
						send(send_message);
					} else {
						DataSingleton.getInstance().put(ss[1],
								ss[2]);
						String send_message = "PUT_SUCCESS "
								+ ss[1] + " " + ss[2];
						send(send_message);
					}
				} else {
					if (ss[2].equals("null")) {
						String send_message = "DELETE_SUCCESS "
								+ ss[1]
								+ " "
								+ DataSingleton.getInstance()
										.get(ss[1]);
						DataSingleton.getInstance().remove(
								ss[1]);
						send(send_message);
					} else {
						DataSingleton.getInstance().put(ss[1],
								ss[2]);
						String send_message = "PUT_UPDATE "
								+ ss[1] + " " + ss[2];
						send(send_message);
					}
				}
			}
			// If this is not the right server, response with
			// NOT RESPONSIBLE and send back the newest meta
			else {
				send("SERVER_NOT_RESPONSIBLE "
						+ metaData.toString());
			}

		}
	}


	private String receive() throws IOException {
		int length;
		byte r[] = new byte[BUF_SIZE];
		length = inputStream.read(r);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char) (r[i]));
		}
		String rec_message = sb.toString();
		return rec_message;
	}

	/**
	 * send data to client
	 * 
	 * @param send_message
	 * @throws IOException
	 */
	private void send(String send_message) throws IOException {
		outputStream.write(send_message.getBytes());
		outputStream.flush();
		System.out.println("SEND '" + send_message + "'");
		logger.info("SEND '" + send_message + "' TO ["
				+ socket.getRemoteSocketAddress() + "]");
	}
	
	
	
	/**
	 * Backup the coordinator node data to two replica node instances
	 */
	public void doBackUp() {
		MetaData metaData = DataSingleton.getInstance()
				.getMetaData();
		
		//TODO get the next two servers
		
		
		
		// "Put" data to the two replica servers
		for (Iterator<String> it = metaData.getCircle().keySet().iterator(); 
				it.hasNext();) {
			String k = it.next();
			String value = metaData.getCircle().get(k);
			String split[] = value.split(":");
			// Connect to server
			CommunicationLogic communication = new CommunicationLogic(split[0],
					Integer.parseInt(split[1]));
			try {
				communication.connect();
				String rec_msg = communication.receive();
				System.out.println(rec_msg);
			
				// Send backup command to server
				communication.send("SERVER backUp");
				String ackMsg = communication.receive();
				// Receive ack
				if (ackMsg.split(" ")[0].equals("BACKUP_SUCCESS")) {
					logger.info("backup data "+ ackMsg.split(" ")[1]+":"+ackMsg.split(" ")[2]+" successfully");
				} else if (ackMsg.split(" ")[0].equals("BACKUP_UPDATE")) {
					logger.info("update data "+ ackMsg.split(" ")[1]+":"+ackMsg.split(" ")[2]+" successfully");
				}
				communication.disconnect();
			} catch (IOException e) {
				System.out.println("ERROR! Cannot connect to server");
				logger.error("ERROR! Cannot connect to server");
			}
		}
	}
	
	
	
	
	/**
	 * ack the PUT Operation(backup)
	 * 
	 * @param message from the coordinator server
	 * @throws IOException
	 * 
	 * send message back to the coordinator
	 */
	private void ackBackUp(String[] ss) throws IOException {
		// Check status
		StatusType status = DataSingleton.getInstance()
				.getStatus();
		if (status.equals(StatusType.valueOf("SERVER_STOPPED"))) {
			send("SERVER_STOPPED");
		} else if (status.equals(StatusType
				.valueOf("SERVER_WRITE_LOCK"))) {
			send("SERVER_WRITE_LOCK");
		} else {
			// If the replica server doesn't hold the key
			if (!DataSingleton.getInstance().containsKey(ss[1])) {
					DataSingleton.getInstance().put(ss[1],
							ss[2]);
					String send_message = "BACKUP_SUCCESS "
							+ ss[1] + " " + ss[2];
					send(send_message);
			} else {
					DataSingleton.getInstance().put(ss[1],
							ss[2]);
					String send_message = "BACKUP_UPDATE "
							+ ss[1] + " " + ss[2];
					send(send_message);
			}
		}
	}
	

}
