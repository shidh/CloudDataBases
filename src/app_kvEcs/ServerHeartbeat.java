package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class ServerHeartbeat extends Thread{
	private int BUF_SIZE = 1024;
	private Socket socket = null;
	private ServerSocket serverSocket = null;
	private InputStream inputStream;
	private Map<String, Timer> serverMap = new HashMap<String, Timer>();
	
	@Override
    public void run() {
		int PORT = 60000;
		// Initiate server socket
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        while(true){
        	try {
				// Accept client's connection
				socket = serverSocket.accept();
			} catch (IOException e) {
				System.out.println("fail to accept");
				e.printStackTrace();
			}
        	if (socket != null) {
        		try {
    				inputStream = socket.getInputStream();
    			} catch (IOException e1) {
    			}
    			try {
    				String rec_message = receive();
    				final String serverPort = rec_message.split(" ")[0];
    				if (serverMap.containsKey(serverPort)) {
    					serverMap.get(serverPort).cancel();
    				}
    				Timer timer = new Timer();
					TimerTask task = new TimerTask() {   
						public void run() {
							ECS.removeTargetNode(serverPort);
							ECS.backupRelevantNodes(serverPort);
						}   
					}; 
					timer.schedule(task, 10000);
					serverMap.put(serverPort, timer);
//    				System.out.println(rec_message);
    			} catch (IOException e1) {
    			}
        	}
        }
    }
	
	private String receive() throws IOException {
		int length;
		byte r[] = new byte[BUF_SIZE];
		length = inputStream.read(r);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char) (r[i]));
		}
		String rec_message = sb.toString();
		return rec_message;
	}

}
