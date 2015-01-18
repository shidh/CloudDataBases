package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import app_kvServer.DataSingleton;

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
				send("starting to listen for changes of "+split[1]);
				
				Map<String,RegisterableData> registerList=DataSingleton.getInstance().getRegisterList();
				if(!registerList.containsKey(split[1])){
					System.out.println("doesn't contains key, create one");
					registerList.put(split[1], new RegisterableData());
				}
				registerList.get(split[1]).addObserver(this);// add observer
			}else if(split[0].equals("deregister")){
				send("stop listening for changes of "+split[1]);
			}
		}
	}

	@Override
	public void update(Observable o, Object arg) {
		System.out.println("Observer being noticified. with msg "+arg);
		send(arg.toString());
	}
	
	/**
	 * send data to client
	 * 
	 * @param send_message
	 * @throws IOException
	 */
	private void send(String send_message) {
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
