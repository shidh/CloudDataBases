package app_kvServer;

import java.util.Map.Entry;

import org.apache.log4j.Logger;

import client.CommunicationLogic;

import com.google.gson.Gson;

public class Heartbeat extends Thread{
	
	private int port;
	Logger logger = Logger.getRootLogger();
	
	Heartbeat(int port){
		this.port = port;
	}
	
	@Override
	public void run(){
		try{
            while(true){
                send();
                Thread.sleep(10000);
            }
            
        }catch(Exception e){
            e.printStackTrace();
        }
	}
	
	public void send(){
		try{
			CommunicationLogic communicate = new CommunicationLogic(
					"localhost", 60000);
			communicate.connect();
			Gson gson=new Gson();
			communicate.send(port + " still alive "+gson.toJson(DataSingleton.getInstance().getResponsibleData()));
			// check data is backup or not
			for (Entry<String, String> entry : DataSingleton.getInstance()
					.getMap().entrySet()) {
				logger.info("Data: " + entry.getKey() + ":"
						+ entry.getValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
