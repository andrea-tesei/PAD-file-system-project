package it.cnr.isti.pad.fs.udpsocket;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import it.cnr.isti.pad.fs.storage.*;

// TODO: implements toString() method for debug purposes
public class StorageMessage extends Message {

	private String Host = null;
	private String fileName = null;
	private Integer Type = null;
	private Integer Command = null;
	private Integer IdRequest = null;
	private Integer returnCode = null;
	private JSONArray Output = null;
	private JSONArray Backup = null;
	private Data Data = null;

	public StorageMessage(JSONObject inputmsg) throws JSONException{
		this.fromJSONObject(inputmsg);
	}

	/**
	 * Storage Message constructor.
	 * 
	 * @param host : host of the sender machine
	 * @param type : type of message
	 * @param idrequest : id of the relative request (in case of type = Response)
	 * @param returnCode : return code of the relative request (in case of type = Response)
	 * @param output : output of the relative request (in case of type = Response)
	 * @param data : data object 
	 * @param backup : all backup copy of the machine for which this node act as replica. (Only with type PUT_BACKUP)
	 */
	public StorageMessage(String host, int type, int idrequest, int Command, int returnCode, JSONArray output, String filename, Data data, JSONArray Backup){
		this.Host = host;
		this.Type = type;
		this.IdRequest = idrequest;
		this.returnCode = returnCode;
		this.Command = Command;
		this.Output = output;
		this.Data = data;
		this.fileName = filename;
		this.Backup = Backup;
	}

	public JSONArray getBackup() {
		return Backup;
	}

	public void setBackup(JSONArray backup) {
		Backup = backup;
	}

	public void setHost(String host) {
		Host = host;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setType(Integer type) {
		Type = type;
	}

	public void setCommand(Integer command) {
		Command = command;
	}

	public void setIdRequest(Integer idRequest) {
		IdRequest = idRequest;
	}

	public void setReturnCode(Integer returnCode) {
		this.returnCode = returnCode;
	}

	public void setOutput(JSONArray output) {
		Output = output;
	}

	public void setData(Data data) {
		Data = data;
	}

	public Integer getCommand() {
		return Command;
	}
	
	public String getFileName() {
		return fileName;
	}

	public String getHost() {
		return Host;
	}

	public Integer getType() {
		return Type;
	}

	public Integer getIdRequest() {
		return IdRequest;
	}

	public Integer getReturnCode() {
		return returnCode;
	}

	public JSONArray getOutput() {
		return Output;
	}

	public Data getData() {
		return Data;
	}

	private void fromJSONObject(JSONObject inputObj) throws JSONException{
		this.IdRequest = inputObj.getInt(StorageMessage.Fields.IDREQ);
		this.Host = inputObj.getString(StorageMessage.Fields.HOST);
		this.Type = inputObj.getInt(StorageMessage.Fields.TYPE);
		this.Command = inputObj.getInt(StorageMessage.Fields.COMMAND);
		this.returnCode = inputObj.getInt(StorageMessage.Fields.RETURNCODE);
		this.Output = inputObj.getJSONArray(StorageMessage.Fields.OUTPUT);
//		if(inputObj.has("data"))
		this.Data = new Data(inputObj.getJSONObject(StorageMessage.Fields.DATA));
//		if(inputObj.has("filename"))
		this.fileName = inputObj.getString(StorageMessage.Fields.FILENAME);
		this.Backup = inputObj.getJSONArray(StorageMessage.Fields.BACKUP);
	}
	
	
	public JSONObject toJSONObject() throws JSONException{
		JSONObject obj = new JSONObject();
		obj.put(StorageMessage.Fields.IDREQ, IdRequest);
		obj.put(StorageMessage.Fields.HOST, Host);
		obj.put(StorageMessage.Fields.TYPE, Type);
		obj.put(StorageMessage.Fields.COMMAND, Command);
		obj.put(StorageMessage.Fields.RETURNCODE, returnCode);
		obj.put(StorageMessage.Fields.OUTPUT, Output);
//		if(this.Data != null)
		obj.put(StorageMessage.Fields.DATA, Data);
//		if(this.fileName != null)
		obj.put(StorageMessage.Fields.FILENAME, fileName);
//		if(this.Backup != null)
		obj.put(StorageMessage.Fields.BACKUP, Backup);
		return obj;

	}

	public static class Fields {
		public static String HOST = "host";
		public static String TYPE = "type";
		public static String COMMAND = "command";
		public static String IDREQ = "idrequest";
		public static String RETURNCODE = "rc";
		public static String OUTPUT = "output";
		public static String DATA = "data";
		public static String FILENAME = "filename";
		public static String BACKUP = "backup";
	}

}
