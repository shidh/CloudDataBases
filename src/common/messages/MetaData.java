package common.messages;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MetaData contains a circle of server nodes whose order is determined by it's
 * hash value
 * 
 * @author Jeff
 * 
 */
public class MetaData {
	/**
	 * Circle is the form of MetaData, whose every server node takes address as
	 * its value and address's corresponding MD5 as its key.
	 */
	private TreeMap<String, String> circle = new TreeMap<String, String>();
	
	public MetaData(){}

	/**
	 * Constructor Construct input string (from byte array) to an instance of
	 * MetaData. Or constructed by ECS.
	 * 
	 * @param constructStr Format: hashValue ip:port hashValue2 ip:port ......
	 */
	public MetaData(String constructStr) {
		String[] ss = constructStr.split(" ");
		for (int i = 0; i < (ss.length); i = i + 2) {
			// ss[i] stands for hash value, ss[i+1] stands for its corresponding
			// address.
			circle.put(ss[i], ss[i + 1]);
		}
	}

	/**
	 * Get circle of the MetaData.
	 * 
	 * @return circle
	 */
	public SortedMap<String, String> getCircle() {
		return circle;
	}

	/**
	 * From byte array to Hex-string
	 * 
	 * @param byteArray
	 * @return Hex-string
	 */
	public String byteArrayToHex(byte[] byteArray) {
		char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'A', 'B', 'C', 'D', 'E', 'F' };
		char[] resultCharArray = new char[byteArray.length * 2];
		int index = 0;
		for (byte b : byteArray) {
			resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
			resultCharArray[index++] = hexDigits[b & 0xf];
		}
		return new String(resultCharArray);
	}

	/**
	 * Get the md5 of the given key.
	 * 
	 * @param k
	 * @return md5 in the form of Hex-string
	 */
	public String computeMd5(String k) {
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 not supported", e);
		}
		md5.reset();
		byte[] keyBytes = null;
		try {
			keyBytes = k.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unknown string :" + k, e);
		}
		md5.update(keyBytes);
		return byteArrayToHex(md5.digest());
	}

	/**
	 * According to a new activated server's address, add this server node to
	 * circle.
	 * 
	 * @param address
	 */
	public void add(String address) {
		circle.put(computeMd5(address), address);
	}

	/**
	 * Remove a server node according to its address.
	 * 
	 * @param node
	 */
	public void remove(String address) {
		circle.remove(computeMd5(address));
	}

	/**
	 * The given key is from a new object, decides which server node it belongs
	 * to and returns the address of this server node.
	 * 
	 * @param key
	 * @return address
	 */
	public String get(String key) {
		String hash = computeMd5(key);
		SortedMap<String, String> tailMap = circle.tailMap(hash);
		if (tailMap.isEmpty()) {
			hash = circle.firstKey();
		} else {
			hash = tailMap.firstKey();
		}
		return circle.get(hash);
	}
	
	
	/**
	 * The given key is already in the circle and returns the address of this server node.
	 * 
	 * @param key
	 * @return address
	 */
	public String getNext(String key) {
		String hash = computeMd5(key);
		String higherKey = circle.higherKey(hash);
		if (higherKey.isEmpty()) {
			hash = circle.firstKey();
		} else {
			hash = higherKey;
		}
		return circle.get(hash);
	}
	
	/**
	 * The given key is from a new object, return address of previous node which is closet to this
	 * new object.
	 * 
	 * @param key
	 * @return address
	 */
	public String getPrevious(String key) {
		String hash = computeMd5(key);
		SortedMap<String, String> headMap = circle.headMap(hash);
		if (headMap.isEmpty()) {
			hash = circle.lastKey();
		} else {
			hash = headMap.lastKey();
		}
		return circle.get(hash);
	}


	/**
	 * MetaDataToString, in order to transfer from servers.
	 * 
	 * @param metaData
	 * @return circle of the metaData to string
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Iterator<String> it = circle.keySet().iterator(); it
				.hasNext();) {
			String k=it.next();
			sb.append(k);
			sb.append(" ");
			sb.append(circle.get(k));
			sb.append(" ");
		}
		return sb.toString();
	}
}
