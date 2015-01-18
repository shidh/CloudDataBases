package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import app_kvServer.DataSingleton;

import com.google.gson.Gson;

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
			serverSocket = new ServerSocket(PORT,-1);
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
    			} catch (IOException e) {
    				System.out.println("fail to get input stream");
    				e.printStackTrace();
    			}
    			try {
    				String rec_message = receive();
    				// parse data
    				String dataJSON=rec_message.split("alive ")[1];
    				Gson gson=new Gson();
    				HashMap<String,String> map=gson.fromJson(dataJSON, HashMap.class);
    				for(Map.Entry<String,String> item:map.entrySet()){
    					if(item.getValue().equals("null")){
    						// delete notification
    						System.out.println("fire delete event of "+item.getKey());
    						if(DataSingleton.getInstance().getRegisterList().containsKey(item.getKey())){
    							RegisterableData rd=DataSingleton.getInstance().getRegisterList().get(item.getKey());
    							rd.setChanged();
    							rd.notifyObservers("NOTIFICATION DELETE "+item.getKey());
    						}
    					}else{
    						// update notification
    						System.out.println("fire update event of "+item.getKey()+" the new value is "+item.getValue());
    						if(DataSingleton.getInstance().getRegisterList().containsKey(item.getKey())){
    							RegisterableData rd=DataSingleton.getInstance().getRegisterList().get(item.getKey());
    							rd.setChanged();
    							rd.notifyObservers("NOTIFICATION UPDATE "+item.getKey()+"="+item.getValue());
    						}
    					}
    				}
    				// heart beat
    				final String serverPort = rec_message.split(" ")[0];
    				if (serverMap.containsKey(serverPort)) {
    					serverMap.get(serverPort).cancel();
    				}
    				Timer timer = new Timer();
					TimerTask task = new TimerTask() {   
						public void run() {
							ServerInfo serverInfo = ECS.removeTargetNode(serverPort);
							ECS.backupRelevantNodes(serverInfo.getAdd() + ":" + serverInfo.getPort());
							// backup method already inside
							ECS.addNode();
						}   
					}; 
					timer.schedule(task, 60000);
					serverMap.put(serverPort, timer);
					socket.close();
//    				System.out.println(rec_message);
    			} catch (IOException e) {
    				System.out.println("fail to read from input stream");
    				e.printStackTrace();
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
