package it.cnr.isti.pad.fs.udpsocket;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class StorageMessage extends Message {
	
	private JSONObject message;
	
	StorageMessage(JSONObject inputmsg){
		message = inputmsg;
	}
	
	StorageMessage(){
		message = new JSONObject();
	}
	
	public void setIdRequest(int id) throws JSONException{
		message.put("idrequest", id);
	}
	
	public int getIdRequest() throws JSONException {
		return message.getInt("idrequest");
	}
	
	public void setHost(String host) throws JSONException{
		message.put("host", host);
	}
	
	public String getHost() throws JSONException{
		return message.getString("host");
	}
	
	public void setType(int type) throws JSONException{
		message.put("type", type);
	}
	
	public int getType() throws JSONException{
		return message.getInt("type");
	}
	
	public void setCommand(int command) throws JSONException{
		message.put("command", command);
	}
	
	public int getCommand() throws JSONException{
		return message.getInt("command");
	}
	
	public void setReturnCode(int rc) throws JSONException{
		message.put("rc", rc);
	}
	
	public int getReturnCode() throws JSONException{
		return message.getInt("rc");
	}
	
	public void setOutput(JSONArray out) throws JSONException{
		message.put("output", out);
	}
	
	public JSONArray getOutput() throws JSONException{
		return message.getJSONArray("output");
	}

}
