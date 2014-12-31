package app_kvServer;

import client.CommunicationLogic;

public class Heartbeat extends Thread{
	
	private int port;
	
	Heartbeat(int port){
		this.port = port;
	}
	
	@Override
	public void run(){
		try{
            while(true){
                send();
                Thread.sleep(2000);
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
			communicate.send(port + " still alive");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
