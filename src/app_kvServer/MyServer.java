package app_kvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

public class MyServer extends Thread {
	private Logger logger=Logger.getRootLogger();
	private int PORT = -1;

	public MyServer(int port) {
		PORT = port;
	}

	@Override
	public void run() {
		ServerSocket serverSocket = null;
		// Initiate server socket
		try {
			serverSocket = new ServerSocket(PORT);
			System.out.println("Start listening to port "+PORT+"...");
			logger.info("Start listening to port "+PORT+"...");
			DataSingleton.getInstance().port=PORT;
		} catch (IOException e1) {
			System.out.println("fail to bind port "+PORT+"...");
			e1.printStackTrace();
		}
		while (true) {
			try {
				// Accept client's connection
				Socket socket = null;
				socket = serverSocket.accept();
				// Generate a new thread to listen to client
				if (socket != null) {
					Thread thread = new Thread(new Listen(socket));
					thread.start();
				}

			} catch (IOException e) {
				System.out.println("fail to accept");
				e.printStackTrace();
			}
		}
	}
}
