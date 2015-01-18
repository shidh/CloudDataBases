package app_kvEcs;

import java.util.Observable;
import java.util.Observer;

public class RegisterableData extends Observable {
	public String value=null;
	public String status="VALID"; // VALID or DELETED
	@Override
	public synchronized void setChanged() {
		super.setChanged();
	}
	
}
