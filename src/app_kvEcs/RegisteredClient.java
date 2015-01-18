package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

public class RegisteredClient extends Thread implements Observer {
	Logger logger=Logger.getRootLogger();
	Socket socket=null;
	InputStream inputStream;
	OutputStream outputStream;
	int BUF_SIZE = 1024;

	public RegisteredClient(Socket socket) {
		super();
		this.socket = socket;
		try {
			inputStream = socket.getInputStream();
			outputStream = socket.getOutputStream();
		} catch (IOException e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
	}
	
	
	/**
	 * Keeps reading from client.
	 */
	@Override
	public void run() {
		super.run();
		while(!socket.isClosed()){
			String rec_msg=receive();
			String split[]=rec_msg.split(" ");
			
			if(split[0].equals("register")){
				
			}else if(split[0].equals("deregister")){
				
			}
		}
	}



	@Override
	public void update(Observable o, Object arg) {
		// TODO Auto-generated method stub

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
	 * Receive message
	 * @return
	 * @throws IOException
	 */
	private String receive() {
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
			logger.info("RECEIVE '" + rec_message + "' FROM ["
					+ socket.getRemoteSocketAddress() + "]");
		} catch (Exception e) {
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());
		}
		return rec_message;

	}
	
}
