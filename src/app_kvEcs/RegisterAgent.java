package app_kvEcs;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

public class RegisterAgent extends Thread {
	static int PORT=60001;
	static Logger logger=Logger.getRootLogger();
	ServerSocket ss;
	
	@Override
	public void run() {
		super.run();
		
		try {
			ss=new ServerSocket(PORT,-1);// -1 for unlimited clients
			
			while(true){
				Socket socket=ss.accept();
				
				RegisteredClient rc=new RegisteredClient(socket);
				new Thread(rc).run();
			}
			
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}finally{
			try {
				ss.close();
			} catch (IOException e) {
				StringWriter errors = new StringWriter();
				e.printStackTrace(new PrintWriter(errors));
				logger.error(errors.toString());
			}
		}
	}

}
