package app_kvServer;

import java.util.Date;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer {
	static int PORT = 50000;

	static private Logger logger = Logger.getRootLogger();
	static String syetemSeperator = System.getProperty("file.separator");
	static String projectRoot = System.getProperty("user.dir");
	
	private static String LOG_DIR=projectRoot + syetemSeperator + "logs"+syetemSeperator +"server"+syetemSeperator;

	/**
	 * Start KV Server at given port
	 * 
	 * @param port
	 *            given port for storage server to operate
	 */
	public KVServer(int port) {
		PORT = port;
		MyServer myServer = new MyServer(PORT);
		logger.info("[START]");
		new Thread(myServer).start();
	}

	public static void main(String[] args) {
		try {
			if (args.length == 2) {
				// Set port and logger level
				PORT = Integer.parseInt(args[0]);
				Level level = Level.toLevel(args[1]);
//				new LogSetup("logs/server/server "+PORT+".log", level);
				// TODO  remember to change log file in the end
				new LogSetup(LOG_DIR+"server"+PORT+".log", level);
			} else {
				System.out.println("ERROR invalid parameter numbers");
				System.out.println("usage:");
				System.out
						.println("java -jar ms2-server.jar <PORT> <LOG LEVEL>");
				System.exit(0);
			}
		} catch (Exception e) {
			System.out.println("ERROR cannot set up server");
			System.out.println("usage:");
			System.out.println("java -jar ms2-server.jar <PORT> <LOG LEVEL>");
		}
		new KVServer(PORT);
	}
}
