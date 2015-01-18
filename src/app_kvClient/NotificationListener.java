package app_kvClient;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import client.CommunicationLogic;

public class NotificationListener extends Thread {
	Logger logger=Logger.getRootLogger();
	CommunicationLogic communicationLogic;
	
	public NotificationListener(CommunicationLogic cl) {
		communicationLogic=cl;
	}

	@Override
	public void run() {
		super.run();
		
		while(!communicationLogic.socket.isClosed()){
			try {
				String rec_msg=communicationLogic.receive();
				System.out.println("EchoClient> "+rec_msg);
			} catch (IOException e) {
				StringWriter errors = new StringWriter();
				e.printStackTrace(new PrintWriter(errors));
				logger.error(errors.toString());
			}
		}
	}

}
