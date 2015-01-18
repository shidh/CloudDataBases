package app_kvServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import app_kvEcs.RegisterableData;

import common.messages.KVMessage.StatusType;
import common.messages.MetaData;

public class DataSingleton {
	Logger logger = Logger.getRootLogger();
	private static DataSingleton dataSingleton = null;

	private Map<String, String> map = new HashMap<String, String>();
	
	private StatusType status=StatusType.SERVER_STOPPED;
	
	private Map<String,RegisterableData> registerList=new HashMap<String,RegisterableData>();
	
	public Map<String, RegisterableData> getRegisterList() {
		return registerList;
	}

	public void setRegisterList(Map<String, RegisterableData> registerList) {
		this.registerList = registerList;
	}

	public int port;
	
	private Map<String, String> tmp=new HashMap<String, String>();
	
	/**
	 * Compares the data to the last retrieved one
	 * @return
	 */
	public Map<String,String> getDataDifference(){
		Map<String,String> result=new HashMap<String, String>();
		
		// put deleted data to be kay:null
		for(Entry<String,String> item:tmp.entrySet()){
			if(metaData.get(item.getKey()).split(":")[1].equals(port+"")){
				if(!map.containsKey(item.getKey())){
					// this key has been deleted
					result.put(item.getKey(), "null");
				}else if(!item.getValue().equals(map.get(item.getKey()))){
					// this key has been updated
					result.put(item.getKey(), map.get(item.getKey()));
				}
			}
		}
		
		tmp=new HashMap<String,String>(map);// copy
		return result;
	}
	
	public Map<String, String> getMap() {
		return map;
	}

	public void setMap(Map<String, String> map) {
		this.map = map;
	}

	public StatusType getStatus() {
		return status;
	}

	public void setStatus(StatusType status) {
		this.status = status;
	}

	private MetaData metaData;

	public MetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(MetaData metaData) {
		this.metaData = metaData;
	}

	private DataSingleton() {
	}

	/**
	 * Get an instance of the data. This instance will be the only instance in
	 * the system.
	 * 
	 * @return Instance of DataSingleton
	 */
	public static DataSingleton getInstance() {
		if (dataSingleton == null) {
			dataSingleton = new DataSingleton();
		}
		return dataSingleton;
	}

	/**
	 * Put or update
	 * 
	 * @param key
	 * @param value
	 */
	synchronized public void put(String key, String value) {
		// TODO If the server status is SERVER_STOPPED or SERVER_WRITE_LOCK we
		// should block user request

		map.put(key, value);
		save();
		return;
	}

	/**
	 * Get value
	 * 
	 * @param key
	 * @return
	 */
	synchronized public String get(String key) {
		// TODO If the server status is SERVER_STOPPED we should block user
		// request
		return map.get(key);
	}

	/**
	 * See if already have the key
	 * 
	 * @param key
	 * @return
	 */
	public boolean containsKey(String key) {
		return map.containsKey(key);
	}

	/**
	 * Remove data
	 * 
	 * @param string
	 */
	synchronized public void remove(String string) {
		// TODO If the server status is SERVER_STOPPED we should block user
		// request
		map.remove(string);
		save();
	}
	
	public void save(){
		
		String rootpath="data";
		File rootfile=new File(rootpath);
		if(!rootfile.exists()){
			rootfile.mkdir();
		}
		String dataPath=rootpath+"/"+port+".txt";
		File dataFile=new File(dataPath);
		if(!dataFile.exists()){
			try {
				dataFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		PrintWriter writer;
		try {
			writer = new PrintWriter(dataPath, "UTF-8");
			for (Entry<String, String> entry : map.entrySet()) {
			    String key = entry.getKey();
			    Object value = entry.getValue();
			    logger.info(key + " = " + value);
				writer.println(key + " = " + value);
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
