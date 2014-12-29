package app_kvEcs;

import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;

public class ECSClient {
	/**
	 * Entrance of the ECS
	 * @param args filename of configuration file
	 */
	public static void main(String[] args) {
		// No configuration file
		if(args.length!=1){
			System.out.println("ERROR! No configuration file");
			return;
		}
		// Setup logger
		try {
			new LogSetup("logs/server/ECS.log", Level.ALL);
		} catch (IOException e) {
			System.out.println("ERROR! Cannot setup logger");
		}
		// Launch Application
		new ECS(args[0]);
	}
}
