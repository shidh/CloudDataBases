package common.messages;

public class KVMessageImp implements KVMessage {
	String key = null;
	String value = null;
	StatusType status = null;
	MetaData metaData = null;

	public KVMessageImp(String RECMessage) {
		String[] ss = RECMessage.split(" ");
		if (ss[0].equals("GET") || ss[0].equals("GET_ERROR")
				|| ss[0].equals("GET_SUCCESS") || ss[0].equals("PUT")
				|| ss[0].equals("PUT_SUCCESS") || ss[0].equals("PUT_UPDATE")
				|| ss[0].equals("PUT_ERROR") || ss[0].equals("DELETE_SUCCESS")
				|| ss[0].equals("DELETE_ERROR")) {
			status = StatusType.valueOf(ss[0]);
			if (ss.length == 3) {
				key = ss[1];
				value = ss[2];
			} else {
				key = ss[1];
				value = null;
			}
		} else if (ss[0].equals("SERVER_STOPPED")
				|| ss[0].equals("SERVER_WRITE_LOCK")) {
			status = StatusType.valueOf(ss[0]);
		} else if (ss[0].equals("SERVER_NOT_RESPONSIBLE")) {
			status = StatusType.valueOf(ss[0]);
			// fetch left ss except ss[0], and use it to construct a new
			// metadata.
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < ss.length; i++) {
				sb.append(ss[i]);
				sb.append(" ");
			}
			// store and update metaData.
			metaData = new MetaData(sb.toString());
		} else
			System.out.println("error status!");
	}

	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return status;
	}

	@Override
	public MetaData getMetaData() {
		// TODO Auto-generated method stub
		return metaData;
	}

	@Override
	public String KVMtoStr(KVMessage kvmessage) {
		StringBuilder sb = new StringBuilder();
		sb.append(kvmessage.getStatus().toString());
		sb.append(" ");
		sb.append(kvmessage.getKey());
		if (kvmessage.getValue() != null) {
			sb.append(" ");
			sb.append(kvmessage.getValue());
		}
		String string = sb.toString();
		return string;
	}

}
