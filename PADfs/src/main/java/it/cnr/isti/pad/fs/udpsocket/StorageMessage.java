package it.cnr.isti.pad.fs.udpsocket;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import it.cnr.isti.pad.fs.storage.*;

// TODO: implements toString() method for debug purposes
public class StorageMessage extends Message {

	private String Host = null;
	private String destinationHost = null;
	private String fileName = null;
	private Integer Type = null;
	private Integer Command = null;
	private Integer IdRequest = null;
	private Integer returnCode = null;
	private Integer handlerCounter = 0;
	private JSONArray Output = null;
	private JSONArray Backup = null;
	private Data Data = null;

	public StorageMessage(JSONObject inputmsg) throws JSONException, IOException{
		this.fromJSONObject(inputmsg);
	}

	/**
	 * Storage Message constructor.
	 * 
	 * @param host : host of the sender machine
	 * @param destinationHost : host of the receiver machine
	 * @param type : type of message
	 * @param idrequest : id of the relative request (in case of type = Response)
	 * @param Command : the command to be executed
	 * @param returnCode : return code of the relative request (in case of type = Response)
	 * @param output : output of the relative request (in case of type = Response)
	 * @param filename
	 * @param data : data object 
	 * @param backup : all backup copy of the machine for which this node act as replica. (Only with type PUT_BACKUP)
	 */
	public StorageMessage(String host, String destinationHost, int type, int idrequest, int Command, int returnCode, JSONArray output, String filename, Data data, JSONArray Backup){
		this.Host = host;
		this.destinationHost = destinationHost;
		this.Type = type;
		this.IdRequest = idrequest;
		this.returnCode = returnCode;
		this.Command = Command;
		this.Output = output;
		this.Data = data;
		this.fileName = filename;
		this.Backup = Backup;
	}
	
	public Integer getHandlerCounter() {
		return handlerCounter;
	}

	public void setHandlerCounter(Integer handlerCounter) {
		this.handlerCounter = handlerCounter;
	}

	public String getDestinationHost() {
		return destinationHost;
	}

	public void setDestinationHost(String destinationHost) {
		this.destinationHost = destinationHost;
	}

	public JSONArray getBackup() {
		return Backup;
	}

	public void setBackup(JSONArray backup) {
		this.Backup = backup;
	}

	public void setHost(String host) {
		this.Host = host;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setType(Integer type) {
		this.Type = type;
	}

	public void setCommand(Integer command) {
		this.Command = command;
	}

	public void setIdRequest(Integer idRequest) {
		this.IdRequest = idRequest;
	}

	public void setReturnCode(Integer returnCode) {
		this.returnCode = returnCode;
	}

	public void setOutput(JSONArray output) {
		this.Output = output;
	}

	public void setData(Data data) {
		this.Data = data;
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

	private void fromJSONObject(JSONObject inputObj) throws JSONException, IOException{
		this.IdRequest = inputObj.getInt(StorageMessage.Fields.IDREQ);
		this.Host = inputObj.getString(StorageMessage.Fields.HOST);
		this.Type = inputObj.getInt(StorageMessage.Fields.TYPE);
		this.Command = inputObj.getInt(StorageMessage.Fields.COMMAND);
		this.returnCode = inputObj.getInt(StorageMessage.Fields.RETURNCODE);
		if(inputObj.has(StorageMessage.Fields.OUTPUT))
			this.Output = inputObj.getJSONArray(StorageMessage.Fields.OUTPUT);
		if(inputObj.has(StorageMessage.Fields.DATA))
			this.Data = new Data(inputObj.getJSONObject(StorageMessage.Fields.DATA));
		if(inputObj.has(StorageMessage.Fields.FILENAME))
			this.fileName = inputObj.getString(StorageMessage.Fields.FILENAME);
		if(inputObj.has(StorageMessage.Fields.BACKUP))
			this.Backup = inputObj.getJSONArray(StorageMessage.Fields.BACKUP);
	}
	
	public JSONObject toJSONObject() throws JSONException{
		JSONObject obj = new JSONObject();
		obj.put(StorageMessage.Fields.IDREQ, IdRequest);
		obj.put(StorageMessage.Fields.HOST, Host);
		obj.put(StorageMessage.Fields.TYPE, Type);
		obj.put(StorageMessage.Fields.COMMAND, Command);
		obj.put(StorageMessage.Fields.RETURNCODE, returnCode);
		if(this.Output != null)
			obj.put(StorageMessage.Fields.OUTPUT, Output);
		if(this.Data != null){
			if(this.Data.getFile() != null){
				obj.put(StorageMessage.Fields.DATA, Data.toJSONObjectWithFile());
			} else
				obj.put(StorageMessage.Fields.DATA, Data.toJSONObject());
		}
		if(this.fileName != null)
			obj.put(StorageMessage.Fields.FILENAME, fileName);
		if(this.Backup != null)
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
