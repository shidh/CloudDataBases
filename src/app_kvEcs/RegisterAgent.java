package app_kvEcs;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

public class RegisterAgent extends Thread {
	static int PORT=60001;
	static Logger logger=Logger.getRootLogger();
	
	@Override
	public void run() {
		super.run();
		
		try {
			ServerSocket ss=new ServerSocket(PORT,-1);// -1 for unlimited clients
		} catch (IOException e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}

}
