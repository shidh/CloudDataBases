package app_kvEcs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

import client.CommunicationLogic;
import common.messages.KVMessage.StatusType;
import common.messages.MetaData;

public class ECS{
	private static Logger logger = Logger.getRootLogger();
	public static MetaData metaData = null;
	private static ArrayList<ServerInfo> serverList = new ArrayList<ServerInfo>();

	private BufferedReader stdBF = new BufferedReader(new InputStreamReader(
			System.in));

	static String syetemSeperator = System.getProperty("file.separator");
	static String projectRoot = System.getProperty("user.dir");
	private static String JAR_DIR = projectRoot + syetemSeperator+"ms3-server.jar";

	public static ArrayList<ServerInfo> result = null;

	public ECS() {
		// Read configuration file and store the ip and port information
		File configurationFile = new File("ecs.config");
		BufferedReader bf;
		try {
			bf = new BufferedReader(new FileReader(configurationFile));
			String line = null;
			// Read server infos
			while ((line = bf.readLine()) != null) {
				// Split
				String split[] = line.split(" ");
				if (split.length != 3) {
					System.out
							.println("ERROR! Illegel configuration file format");
					System.exit(0);
				}
				String serverName = split[0];
				String serverAdd = split[1];
				String serverPort = split[2];

				// Save each server info into a list
				ServerInfo serverInfo = new ServerInfo(serverName, serverAdd,
						serverPort);
				serverList.add(serverInfo);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

	/**
	 * Constructor
	 * 
	 * @param string
	 *            filename of configuration file
	 */
	public ECS(String string) {
		// Read configuration file and store the ip and port information
		File configurationFile = new File(string);
		try {
			BufferedReader bf = new BufferedReader(new FileReader(
					configurationFile));
			String line = null;
			// Read server infos
			while ((line = bf.readLine()) != null) {
				// Split
				String split[] = line.split(" ");
				if (split.length != 3) {
					System.out
							.println("ERROR! Illegel configuration file format");
					System.exit(0);
				}
				String serverName = split[0];
				String serverAdd = split[1];
				String serverPort = split[2];

				// Save each server info into a list
				ServerInfo serverInfo = new ServerInfo(serverName, serverAdd,
						serverPort);
				serverList.add(serverInfo);
			}
			// heart beat thread
			ServerHeartbeat serverHeartbeat = new ServerHeartbeat();
			new Thread(serverHeartbeat).start();
			
			// register thread
			RegisterAgent ra=new RegisterAgent();
			new Thread(ra).start();
			
			// Command line user interface
			CommandLine();
		} catch (FileNotFoundException e) {
			System.out.println("ERROR! Cannot read configuration file");
			logger.error("ERROR! Cannot read configuration file");
			return;
		} catch (IOException e) {
			System.out.println("ERROR! IO");
			logger.error("ERROR! IO");
			return;
		}
	}

	/**
	 * User interface
	 */
	private void CommandLine() {
		while (true) {
			System.out.print("ECS>> ");
			String stdStr = readSTD();
			String token[] = stdStr.split(" ");

			if (token[0].equals("initService")) {
				// Validate the command
				if (token.length != 2) {
					System.out.println("Illegal number of parameters");
					continue;
				}
				int m = 0;
				try {
					m = Integer.parseInt(token[1]);
				} catch (Exception e) {
					System.out.println("Not a number");
					continue;
				}

				initService(m);
			} else if (token[0].equals("start")) {
				// Start the selected servers
				start();
			} else if (token[0].equals("stop")) {
				stop();
			} else if (token[0].equals("shutDown")) {
				shutDown();
			} else if (token[0].equals("addNode")) {
				addNode();
			} else if (token[0].equals("removeNode")) {
				removeNode();
			} else if (token[0].equals("quit")) {
				break;
			} else if (token[0].equals("help")){
				/**
			     * help information
			     */
		        StringBuilder sb = new StringBuilder();
		        sb.append("ECS CLIENT HELP (Usage):\n");
		        sb.append("-----------------------------------------------------------------------------------------------\n");
		        sb.append("initService <number>");
		        sb.append("\t Randomly choose <numberOfNodes> servers from the available machines \n");
		        sb.append("\t\t and start the KVServer by issuing a SSH call to the\n");
		        sb.append("start");
		        sb.append("\t\t Starts the storage service by calling start() on all KVServer instances\n");
		        sb.append("stop");
		        sb.append("\t\t Stops the service; all participating KVServers are stopped for\n");
		        sb.append("\t\t processing client requests but the processes remain running. \n");
		        sb.append("shutdown");
		        sb.append("\t Stops all server instances and exits the remote processes. \n");
		        sb.append("addNode");
		        sb.append("\t\t Add a new node to the storage service at an arbitrary position.\n");
		        sb.append("removeNode");
		        sb.append("\t Remove a node from the storage service at an arbitrary position.\n");
		        sb.append("logLevel");
		        sb.append("\t Sets the logger to the specified log level");
		        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		        sb.append("quit ");
		        sb.append("\t\t Tears down the active connection to the server and exits the program.");
		        logger.info(sb.toString());
			    
			}
		}
		System.out.println("ECS is stopped.");
	}

	/**
	 * Read from standard input
	 * 
	 * @return
	 */
	private String readSTD() {
		String stdStr = "";
		try {
			stdStr = stdBF.readLine();
		} catch (IOException e) {
			System.out.println("ERROR! Failed to read from standard input");
			logger.error("ERROR! Failed to read from standard input");
			return null;
		}
		return stdStr;
	}

	/**
	 * Randomly choose <numberOfNodes> servers from the available machines and
	 * start the KVServer by issuing a SSH call to the respective machine. This
	 * call launches the server.
	 * 
	 * @param numberOfNodes
	 */
	public void initService(int numberOfNodes) {
		// Number of nodes cannot be bigger than the max number
		if (numberOfNodes > serverList.size()) {
			System.out.println("Don't have enough servers");
			return;
		}
		// Random numberOfNodes of server to run
		Random random = new Random();
		// Clone a copy of server list
		ArrayList<ServerInfo> temp = new ArrayList<ServerInfo>();
		for (ServerInfo info : serverList) {
			temp.add(info.clone());
		}
		result = new ArrayList<ServerInfo>();
		int size = temp.size();
		for (int i = 0; i < numberOfNodes; i++) {
			// Get a random position
			int position = random.nextInt(size - i);
			// Store
			result.add(temp.get(position).clone());
			// Replace the position with the last unused element, if it is the
			// last one, don't do it
			if (i != numberOfNodes - 1) {
				temp.remove(position);
				temp.add(position, temp.get(size - 2 - i).clone());
			}
		}
		// Generate consistent hash ring and corresponding meta data
		metaData = new MetaData();
		// Run script to open server program in the remote computer
		Process proc;
		// TODO ssh connection without password
		for (ServerInfo info : result) {
			String command = "";
			// for ssh
			//command += "ssh localhost java -jar ";
			// for local
			command += "java -jar ";
			
			command += JAR_DIR + " ";
			command += info.getPort() + "  ALL\n";

			Runtime run = Runtime.getRuntime();
			try {
				System.out.println(command);
				proc = run.exec(command);
			} catch (IOException e) {
				System.out.println("ERROR! Failed to run script");
				logger.error("ERROR! Failed to run script");
				return;
			}
			// Add one meta data
			metaData.add(info.getAdd() + ":" + info.getPort());
		}
		// Wait for latency
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// Communicate to each server and send meta data to each of them
		for (ServerInfo info : result) {
			CommunicationLogic communicate = new CommunicationLogic(
					info.getAdd(), Integer.parseInt(info.getPort()));
			try {
				communicate.connect();
				String rec_msg = communicate.receive();
				System.out.println(rec_msg);
				communicate.send("ECS initService \n" + metaData.toString());
				String ackMsg = communicate.receive();
				if (ackMsg.equals("ack")) {
					System.out.println("ECS>> Meta data sended to server "
							+ info.getAdd() + ":" + info.getPort()
							+ " successfully");
				} else {
					System.out
							.println("ERROR! Failed to send meta data to server "
									+ info.getAdd() + ":" + info.getPort());
				}
				communicate.disconnect();
			} catch (IOException e) {
				System.out
						.println("ERROR! Cannot connect to server, operation aborted");
				logger.error("ERROR! Cannot connect to server, operation aborted");
				return;
			}

		}
	}

	/**
	 * Starts the storage service by calling start() on all KVServer instances
	 * that participate in the service.
	 */
	public void start() {
		// Send start message to every server
		for (Iterator<String> it = metaData.getCircle().keySet().iterator(); it
				.hasNext();) {
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
				// Send start command to server
				communication.send("ECS start");
				String ackMsg = communication.receive();
				// Receive ack
				if (ackMsg.equals("ack")) {
					System.out.println("Start server successfully");
				}
				communication.disconnect();
			} catch (IOException e) {
				System.out.println("ERROR! Cannot connect to server");
				logger.error("ERROR! Cannot connect to server");
			}
		}
	}

	/**
	 * Stops the service; all participating KVServers are stopped for processing
	 * client requests but the processes remain running.
	 */
	public void stop() {
		// Send stop message to every server
		for (Iterator<String> it = metaData.getCircle().keySet().iterator(); it
				.hasNext();) {
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
				// Send stop command to server
				communication.send("ECS stop");
				String ackMsg = communication.receive();
				// Receive ack
				if (ackMsg.equals("ack")) {
					System.out.println("Stop server successfully");
				}
				communication.disconnect();
			} catch (IOException e) {
				System.out.println("ERROR! Cannot connect to server");
				logger.error("ERROR! Cannot connect to server");
			}
		}
	}

	/**
	 * Stops all server instances and exits the remote processes.
	 */
	public void shutDown() {
		// Send shutdown message to every server
		for (Iterator<String> it = metaData.getCircle().keySet().iterator(); it
				.hasNext();) {
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
				// Send shutdown command to server
				communication.send("ECS shutDown");
				String ackMsg = communication.receive();
				// Receive ack
				if (ackMsg.equals("ack")) {
					System.out.println("Shutdown server successfully");
				}
				communication.disconnect();
			} catch (IOException e) {
				System.out.println("ERROR! Cannot connect to server");
				logger.error("ERROR! Cannot connect to server");
			}
		}
		// Remove server from meta data according to unavailable server
		// list(result)
		for (ServerInfo info : result) {
			metaData.remove(info.getAdd() + ":" + info.getPort());
		}
		// Remove servers from unavailable list(result)
		result.removeAll(result);
	}

	/**
	 * Add a new node to the storage service at an arbitrary position.
	 */
	public static void addNode() {
		// Clone a copy of server list
		ArrayList<ServerInfo> temp = new ArrayList<ServerInfo>(serverList);
		// Leave out unavailable servers
		Iterator<ServerInfo> it = temp.iterator();
		while (it.hasNext()) {
			ServerInfo info = it.next();
			Iterator<ServerInfo> itResult = result.iterator();
			while (itResult.hasNext()) {
				ServerInfo infoResult = itResult.next();
				if (info.equals(infoResult)) {
					it.remove();
				}
			}
		}
		// Random numberOfNodes of server to run
		Random random = new Random();
		// Random number according to size
		int seed = temp.size();
		// No more server
		if (seed == 0) {
			System.out.println("No more available server");
			return;
		}
		int randomIndex = random.nextInt(seed);
		
		// Start a server
		String command = "";
		// for ssh
		command += "ssh localhost java -jar ";
		// for local
//		command += "java -jar ";
		
		command += JAR_DIR + " ";
		command += temp.get(randomIndex).getPort() + "  ALL\n";
		// Run script to open server program in the remote computer
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
		// Find the next server in the consistent hash ring and store it
		String nextServer = metaData.get(temp.get(randomIndex).getAdd() + ":"
				+ temp.get(randomIndex).getPort());
		// Add one meta data
		metaData.add(temp.get(randomIndex).getAdd() + ":"
				+ temp.get(randomIndex).getPort());
		// Wait for latency
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// Communicate server
		CommunicationLogic communicate = new CommunicationLogic(temp.get(
				randomIndex).getAdd(), Integer.parseInt(temp.get(randomIndex)
				.getPort()));
		try {
			communicate.connect();
			String rec_msg = communicate.receive();
			System.out.println(rec_msg);
			communicate.send("ECS addNode \n" + metaData.toString());
			String ackMsg = communicate.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> Meta data sended to server "
						+ temp.get(randomIndex).getAdd() + ":"
						+ temp.get(randomIndex).getPort() + " successfully");
			} else {
				System.out.println("ERROR! Failed to send meta data to server "
						+ temp.get(randomIndex).getAdd() + ":"
						+ temp.get(randomIndex).getPort());
			}
			communicate.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		
		
		// Communicate with new node
		CommunicationLogic communicateStart = new CommunicationLogic(
				temp.get(randomIndex).getAdd(), Integer.parseInt(temp.get(randomIndex).getPort()));
		try {
			communicateStart.connect();
			String rec_msg = communicateStart.receive();
			System.out.println(rec_msg);
			communicateStart.send("ECS start");
			String ackMsg = communicateStart.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> server " + temp.get(randomIndex).getAdd() + ":"
						 + temp.get(randomIndex).getPort() + " backup successfully!");
				logger.info("ECS>> server " + temp.get(randomIndex).getAdd() + ":"
						 + temp.get(randomIndex).getPort() + " backup successfully!");
			} else {
				System.out.println("ECS>> server " + temp.get(randomIndex).getAdd() + ":"
						 + temp.get(randomIndex).getPort() + " backup failed!");
				logger.error("ECS>> server " + temp.get(randomIndex).getAdd() + ":"
						 + temp.get(randomIndex).getPort() + " backup failed!");
			} 
			communicateStart.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		
		// Update meta data to all server
		for (ServerInfo info : result) {
			// Communicate server
			CommunicationLogic communicateUpdate = new CommunicationLogic(
					info.getAdd(), Integer.parseInt(info.getPort()));
			try {
				communicateUpdate.connect();
				String rec_msg = communicateUpdate.receive();
				System.out.println(rec_msg);
				communicateUpdate.send("ECS updateMetaData \n"
						+ metaData.toString());
				String ackMsg = communicateUpdate.receive();
				if (ackMsg.equals("ack")) {
					System.out.println("ECS>> Meta data update to server "
							+ info.getAdd() + ":" + info.getPort()
							+ " successfully");
				} else {
					System.out
							.println("ERROR! Failed to send meta data to server "
									+ info.getAdd() + ":" + info.getPort());
				}
				communicateUpdate.disconnect();
			} catch (IOException e) {
				System.out
						.println("ERROR! Cannot connect to server, operation aborted");
				logger.error("ERROR! Cannot connect to server, operation aborted");
				return;
			}
		}
		// Add new node to available list(result)
		ServerInfo randomServer = temp.get(randomIndex);
		result.add(new ServerInfo(randomServer.getName(),
				randomServer.getAdd(), randomServer.getPort()));
		// Move data from next server to newly added server
		String splitNextServer[] = nextServer.split(":");
		// Communicate next server
		CommunicationLogic communicateNext = new CommunicationLogic(
				splitNextServer[0], Integer.parseInt(splitNextServer[1]));
		try {
			communicateNext.connect();
			String rec_msg = communicateNext.receive();
			System.out.println(rec_msg);
			communicateNext.send("ECS moveDataTo "
					+ temp.get(randomIndex).getAdd() + ":"
					+ temp.get(randomIndex).getPort());
			String ackMsg = communicateNext.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> Data already moved to server "
						+ splitNextServer[0] + ":" + splitNextServer[1]
						+ " successfully");
				logger.info("ECS>> Data already moved to server "
						+ splitNextServer[0] + ":" + splitNextServer[1]
						+ " successfully");
			} else {
				System.out.println("ERROR! Failed to send meta data to server "
						+ splitNextServer[0] + ":" + splitNextServer[1]);
				logger.info("ERROR! Failed to send meta data to server "
						+ splitNextServer[0] + ":" + splitNextServer[1]);
			}
			communicateNext.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		
		
		String newNodeAddress = temp.get(randomIndex).getAdd() + ":"+ temp.get(randomIndex).getPort();
		// inform relevant nodes to backup
		backupRelevantNodes(newNodeAddress);
		
	}

	/**
	 * Remove a node from the storage service at an arbitrary position.
	 */
	public void removeNode() {
		// Random a server to remove from unavailable server list(result)
		int seed = result.size();
		Random random = new Random();
		int randomPosition = random.nextInt(seed);
		ServerInfo randomServer = result.get(randomPosition);
		String randomServerIP = randomServer.getAdd();
		String randomServerPort = randomServer.getPort();
		// Remove node from meta data
		metaData.remove(randomServerIP + ":" + randomServerPort);
		// Update meta data to all server
		for (ServerInfo info : result) {
			// Communicate server
			CommunicationLogic communicateUpdate = new CommunicationLogic(
					info.getAdd(), Integer.parseInt(info.getPort()));
			try {
				communicateUpdate.connect();
				String rec_msg = communicateUpdate.receive();
				System.out.println(rec_msg);
				communicateUpdate.send("ECS updateMetaData \n"
						+ metaData.toString());
				String ackMsg = communicateUpdate.receive();
				if (ackMsg.equals("ack")) {
					System.out.println("ECS>> Meta data update to server "
							+ info.getAdd() + ":" + info.getPort()
							+ " successfully");
				} else {
					System.out
							.println("ERROR! Failed to send meta data to server "
									+ info.getAdd() + ":" + info.getPort());
				}
				communicateUpdate.disconnect();
			} catch (IOException e) {
				System.out
						.println("ERROR! Cannot connect to server, operation aborted");
				logger.error("ERROR! Cannot connect to server, operation aborted");
				return;
			}
		}
		// Get next server of the random server
		String nextServer = metaData.get(randomServerIP + ":"
				+ randomServerPort);
		String split[] = nextServer.split(":");
		String nextServerIP = split[0];
		String nextServerPort = split[1];
		// Communicate to the random server and send the next server's address
		// to it, then it will remove all the data to the next server itself
		CommunicationLogic communicateRandomServer = new CommunicationLogic(
				randomServerIP, Integer.parseInt(randomServerPort));
		try {
			communicateRandomServer.connect();
			String rec_msg = communicateRandomServer.receive();
			System.out.println(rec_msg);
			communicateRandomServer.send("ECS removeNode to " + nextServerIP
					+ ":" + nextServerPort);
			String ackMsg = communicateRandomServer.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> Server" + randomServerIP + ":"
						+ randomServerPort + " removed successfully ");
			} else {
				System.out.println("ERROR! Failed to remove server "
						+ randomServerIP + ":" + randomServerPort);
			}
			communicateRandomServer.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		// Remove server from unavailable server list(result)
		result.remove(randomPosition);
		// inform relevant nodes to backup
		backupRelevantNodes(randomServerIP + ":" + randomServerPort);
	}

	/**
	 * Remove a node at a specified position.
	 */
	static public ServerInfo removeTargetNode(String port) {
		String targetServerIP = null;
		String targetServerPort = null;
		ArrayList<ServerInfo> delServerList = new ArrayList<ServerInfo>();
		if (result.size() == 0) {
			logger.error("No available servers!");
//			System.out.println("No available servers!");
			return null;
		}
		for (ServerInfo info : result) {
			if (info.getPort().equals(port)){
				targetServerIP = info.getAdd();
				targetServerPort = port;
				delServerList.add(info);
				System.out.println("The following server is crashed: " + info);
				logger.info("The following server is crashed: " + info);
			}
		}
//		serverList.remove(delServerList.get(0));
		result.removeAll(delServerList);
		// Remove node from meta data
		metaData.remove(targetServerIP + ":" + targetServerPort);
		// Update meta data to all available server
		for (ServerInfo info : result) {
			
			// Communicate server
			CommunicationLogic communicateUpdate = new CommunicationLogic(
					info.getAdd(), Integer.parseInt(info.getPort()));
			try {
				communicateUpdate.connect();
				String rec_msg = communicateUpdate.receive();
				System.out.println(rec_msg);
				communicateUpdate.send("ECS updateMetaData \n"
						+ metaData.toString());
				String ackMsg = communicateUpdate.receive();
				if (ackMsg.equals("ack")) {
					System.out.println("ECS>> Meta data update to server "
							+ info.getAdd() + ":" + info.getPort()
							+ " successfully");
				} else {
					System.out
							.println("ERROR! Failed to send meta data to server "
									+ info.getAdd() + ":" + info.getPort());
				}
				communicateUpdate.disconnect();
			} catch (IOException e) {
				System.out
						.println("ERROR! Cannot connect to server, operation aborted");
				logger.error("ERROR! Cannot connect to server, operation aborted");
				return null;
			}
		}
		return delServerList.get(0);
	}
	
	/**
	 * Backup relevant nodes of crashed node, namely one node next and two nodes before the
	 * crashed node.
	 * @param address
	 */
	static public void backupRelevantNodes(String address) {
		// there are three relevant servers to be backed up
		String crashedAddress = address;
		String address1;
		if(metaData.get(crashedAddress).equals(crashedAddress)){
			// after added node
			address1 = metaData.getNext(crashedAddress);
		} else {
			// after node crashed
			address1 = metaData.get(crashedAddress);
		}
		String address2 = metaData.getPrevious(crashedAddress);
		String address3 = metaData.getPrevious(address2);
		logger.info("Notify server to backup: " + address1);
		logger.info("Notify server to backup: " + address2);
		logger.info("Notify server to backup: " + address3);
		
		// Communicate with address1
		CommunicationLogic communicateBackup1 = new CommunicationLogic(
				address1.split(":")[0], Integer.parseInt(address1.split(":")[1]));
		try {
			communicateBackup1.connect();
			String rec_msg = communicateBackup1.receive();
			System.out.println(rec_msg);
			communicateBackup1.send("ECS backup");
			String ackMsg = communicateBackup1.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> server " + address1 + " backup successfully!");
				logger.info("ECS>> server " + address1 + " backup successfully!");
			} else {
				System.out.println("ECS>> server " + address1 + " backup failed!");
				logger.error("ECS>> server " + address1 + " backup failed!");
			} 
			communicateBackup1.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		
		// Communicate with address2
		CommunicationLogic communicateBackup2 = new CommunicationLogic(
				address2.split(":")[0], Integer.parseInt(address2.split(":")[1]));
		try {
			communicateBackup2.connect();
			String rec_msg = communicateBackup2.receive();
			System.out.println(rec_msg);
			communicateBackup2.send("ECS backup");
			String ackMsg = communicateBackup2.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> server " + address2 + " backup successfully!");
				logger.info("ECS>> server " + address2 + " backup successfully!");
			} else {
				System.out.println("ECS>> server " + address2 + " backup failed!");
				logger.error("ECS>> server " + address2 + " backup failed!");
			} 
			communicateBackup2.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
		
		// Communicate with address3
		CommunicationLogic communicateBackup3 = new CommunicationLogic(
				address3.split(":")[0], Integer.parseInt(address3.split(":")[1]));
		try {
			communicateBackup3.connect();
			String rec_msg = communicateBackup3.receive();
			System.out.println(rec_msg);
			communicateBackup3.send("ECS backup");
			String ackMsg = communicateBackup3.receive();
			if (ackMsg.equals("ack")) {
				System.out.println("ECS>> server " + address3 + " backup successfully!");
				logger.info("ECS>> server " + address3 + " backup successfully!");
			} else {
				System.out.println("ECS>> server " + address3 + " backup failed!");
				logger.error("ECS>> server " + address3 + " backup failed!");
			} 
			communicateBackup3.disconnect();
		} catch (IOException e) {
			System.out
					.println("ERROR! Cannot connect to server, operation aborted");
			logger.error("ERROR! Cannot connect to server, operation aborted");
			return;
		}
//		System.out.println("Backup: " + address1);
//		System.out.println("Backup: " + address2);
//		System.out.println("Backup: " + address3);
	}
}
