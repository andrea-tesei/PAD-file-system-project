package it.cnr.isti.pad.fs.udpsocket;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import it.cnr.isti.pad.fs.storage.*;

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

	/**
	 * Storage Message constructor.
	 * @param inputmsg the JSONObject representing a received message. 
	*/
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
	
	/**
	 * Function getHandlerCounter. Retrieve the number of attempts this message has been handled. 
	 * @return counter of times this message has been handled.
	 */
	public Integer getHandlerCounter() {
		return handlerCounter;
	}

	/**
	 * Function setHandlerCounter. Set the number of handler attempts already done.
	 * @param handlerCounter the new value for the counter.
	 */
	public void setHandlerCounter(Integer handlerCounter) {
		this.handlerCounter = handlerCounter;
	}

	/**
	 * Function getDestinationHost. Retrieve the destination host of this message.
	 * @return the destination ip for this message.
	 */
	public String getDestinationHost() {
		return destinationHost;
	}

	/**
	 * Function setDestinationHost. Set the destination host for this message.
	 * @param destinationHost the destination ip of this message.
	 */
	public void setDestinationHost(String destinationHost) {
		this.destinationHost = destinationHost;
	}

	/**
	 * Function getBackup. Retrieve the set of backup files in a PUT_BACKUP message type.
	 * @return the array of backup Data objects contained in this Message.
	 */
	public JSONArray getBackup() {
		return Backup;
	}

	/**
	 * Function setBackup. Set the array of backup Data objects for a PUT_BACKUP message.
	 * @param backup the array of backup Data objects.
	 */
	public void setBackup(JSONArray backup) {
		this.Backup = backup;
	}

	/**
	 * Function getHost. Retrieve the ip address of the sender.
	 * @return the ip address of the sender.
	 */
	public String getHost() {
		return Host;
	}
	
	/**
	 * Function setHost. Set the ip address of the sender node.
	 * @param host the ip address of the sender node.
	 */
	public void setHost(String host) {
		this.Host = host;
	}
	
	/**
	 * Function getFileName. Retrieve the file name for the file contained in this message.
	 * @return the name of the file contained in this message.
	 */
	public String getFileName() {
		return fileName;
	}


	/**
	 * Function setFileName. Set the filename for the file contained into this Message.
	 * @param fileName the filename to be set.
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	/**
	 * Function getType. Retrieve the type of this message. @see it.cnr.isti.pad.fs.udpsocket.Message.Type
	 * @return the id of the message type.
	 */
	public Integer getType() {
		return Type;
	}

	/**
	 * Function setType. Set the type of this Message. @see it.cnr.isti.pad.fs.udpsocket.Message.Type
	 * @param type the id of the type for this Message.
	 */
	public void setType(Integer type) {
		this.Type = type;
	}
	
	/**
	 * Function getCommand. Retrieve the command represented by this message.
	 * @return the id of the command in this message. @see it.cnr.isti.pad.fs.udpsocket.Message.Command
	 */
	public Integer getCommand() {
		return Command;
	}

	/**
	 * Function setCommand. Set the command to be executed with this message. @see it.cnr.isti.pad.fs.udpsocket.Message.Command
	 * @param command the id of the command for this message.
	 */
	public void setCommand(Integer command) {
		this.Command = command;
	}
	
	/**
	 * Function getReturnCode. Retrieve the return code for this message. @see it.cnr.isti.pad.fs.udpsocket.Message.ReturnCode
	 * @return the id of the return code contained in this message.
	 */
	public Integer getReturnCode() {
		return returnCode;
	}

	/**
	 * Function setReturnCode. Set the return code for this message. @see it.cnr.isti.pad.fs.udpsocket.Message.ReturnCode
	 * @param returnCode the id of the return code for this message.
	 */
	public void setReturnCode(Integer returnCode) {
		this.returnCode = returnCode;
	}
	
	/**
	 * Function getData. Retrieve the Data object contained in this message.
	 * @return Data object contained in this message.
	 */
	public Data getData() {
		return Data;
	}

	/**
	 * Function setData. Set the Data object contained in this Message.
	 * @param data
	 */
	public void setData(Data data) {
		this.Data = data;
	}

	/**
	 * Function getIdRequest. Retrieve the unique id of this request.
	 * @return the unique id of this message request.
	 */
	public Integer getIdRequest() {
		return IdRequest;
	}
	
	/**
	 * Function setIdRequest. Set the unique id of this request message.
	 * @param idRequest the id of the request to be set.
	 */
	public void setIdRequest(Integer idRequest) {
		this.IdRequest = idRequest;
	}

	/**
	 * Function getOutput. Retrieve the output contained in this message.
	 * @return the JSONArray of the output contained in this message.
	 */
	public JSONArray getOutput() {
		return Output;
	}
	
	/**
	 * Function setOutput. Set the output field for this message, useful to gives back a message explanation of outcome to the requestor. 
	 * @param output to be set in the output field.
	 */
	public void setOutput(JSONArray output) {
		this.Output = output;
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
