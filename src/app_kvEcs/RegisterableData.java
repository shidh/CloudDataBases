package app_kvEcs;

import java.util.Observable;
import java.util.Observer;

public class RegisterableData extends Observable {
	public String value=null;
	public String status="VALID"; // VALID or DELETED
	public RegisterableData(String string) {
		value=string;
	}
	@Override
	public synchronized void setChanged() {
		super.setChanged();
	}
	
}
