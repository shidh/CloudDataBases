package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class CommunicationLogic {
	Logger logger=Logger.getRootLogger();
	
	static String ip = null;
	static int port;
	
	static Socket socket = null;
	static InputStream socketInputStream = null;
	static OutputStream socketOutputStream = null;

	public CommunicationLogic(String IP, int PORT) {
		ip = IP;
		port = PORT;
	}

	public boolean isConnect() {
		if (!socket.isClosed())
			return true;
		else
			return false;
	}

	public void connect() throws IOException {
		try {
			System.out.println(ip+":"+port);
			socket = new Socket(ip, port);
		} catch (UnknownHostException e) {
			throw e;
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to connect to the target");
			throw e;
		}
		// using byte stream
		// Get input stream
		try {
			socketInputStream = socket.getInputStream();
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to get socket input stream");
			throw e;
		}
		// Get output stream
		try {
			socketOutputStream = socket.getOutputStream();
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to get Output stream");
			throw e;
		}
	}

	public void disconnect() throws IOException {
		try {
			socketInputStream.close();
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to close socketInputStream");
			throw e;
		}

		try {
			socketOutputStream.close();
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to close socketInputStream");
			throw e;
		}

		try {
			socket.close();
		} catch (IOException e) {
			System.out.println("EchoClient> Failed to close socket");
			throw e;
		}
	}

	public void send(String msg) throws IOException {
		byte b[] = msg.getBytes();
		socketOutputStream.write(b);
		socketOutputStream.flush();
		logger.info("SEND '"+msg+"' TO ["+socket.getRemoteSocketAddress()+"]");
	}
	

	public String receive() throws IOException {
		byte b[] = new byte[1000];
		int length;
		length = socketInputStream.read(b);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char) (b[i]));
		}
		if(!sb.toString().trim().equals("")){
			logger.info("RECEIVE '"+sb.toString()+"' FROM ["+socket.getRemoteSocketAddress()+"]");
		}
		return sb.toString();
	}
}
