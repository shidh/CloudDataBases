package app_kvEcs;

import common.messages.KVMessage.StatusType;

public class ServerInfo {
	private String name="";
	private String add="";
	private String port="";
	public ServerInfo(String name, String add, String port) {
		super();
		this.name = name;
		this.add = add;
		this.port = port;
	}
	public String getName() {
		return name;
	}
	@Override
	public boolean equals(Object obj) {
		ServerInfo info=(ServerInfo)obj;
		if(add.equals(info.getAdd())&&port.equals(info.getPort())){
			return true;
		}
		return false;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAdd() {
		return add;
	}
	public void setAdd(String add) {
		this.add = add;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	@Override
	public String toString() {
		return name+" "+add+" "+port;
	}

	public ServerInfo clone() {
		return new ServerInfo(name,add,port);
	}
}
