package testing;

import app_kvClient.KVClient;

public class Test {

	public static void main(String[] args) {
		KVClient client=new KVClient();
		try {
			client.connect("localhost", 50001);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < 1000; i++) {
			try {
				client.put(i+"", i+"");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			client.disconnect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
