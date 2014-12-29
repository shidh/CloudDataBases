package app_kvServer;

import java.util.HashMap;
import java.util.Map;

import common.messages.KVMessage.StatusType;
import common.messages.MetaData;

public class DataSingleton {
	private static DataSingleton dataSingleton = null;

	private Map<String, String> map = new HashMap<String, String>();
	
	private StatusType status=StatusType.SERVER_STOPPED;
	
	
	
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
	}
}
