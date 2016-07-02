package it.cnr.isti.pad.fs.datatypes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;



@JsonIgnoreProperties(ignoreUnknown = true)
public class StorageMessage{

	private String Host;
	private String destinationhost;
	private String filename;
	private Integer Type;
	private Integer Command;
	private Integer idrequest;
	private Integer rc;
	private Integer handlerCounter = 0;
	private JsonNode Output;
	private JsonNode Backup;
	private Data Data;

	public StorageMessage(){
		
	}
	
	public StorageMessage(JsonNode inputJson){
		this.fromJsonNode(inputJson);
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
	public StorageMessage(String host, String destinationHost, int type, int idrequest, int Command, int returnCode, JsonNode output, String filename, Data data, JsonNode Backup){
		this.Host = host;
		this.destinationhost = destinationHost;
		this.Type = type;
		this.idrequest = idrequest;
		this.rc = returnCode;
		this.Command = Command;
		this.Output = output;
		this.Data = data;
		this.filename = filename;
		this.Backup = Backup;
	}
	
	public Integer getHandlerCounter() {
		return handlerCounter;
	}

	public void setHandlerCounter(Integer handlerCounter) {
		this.handlerCounter = handlerCounter;
	}

	public void setType(Integer type) {
		this.Type = type;
	}

	public void setCommand(Integer command) {
		this.Command = command;
	}

	public Integer getCommand() {
		return Command;
	}
	
	public Integer getType() {
		return Type;
	}
	
	public String getHost() {
		return Host;
	}

	public void setHost(String host) {
		Host = host;
	}

	public String getDestinationhost() {
		return destinationhost;
	}

	public void setDestinationhost(String destinationhost) {
		this.destinationhost = destinationhost;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public Integer getIdrequest() {
		return idrequest;
	}

	public void setIdrequest(Integer idrequest) {
		this.idrequest = idrequest;
	}

	public Integer getRc() {
		return rc;
	}

	public void setRc(Integer rc) {
		this.rc = rc;
	}

	public JsonNode getOutput() {
		return Output;
	}

	public void setOutput(JsonNode output) {
		Output = output;
	}

	public JsonNode getBackup() {
		return Backup;
	}

	public void setBackup(JsonNode backup) {
		Backup = backup;
	}

	public Data getData() {
		return Data;
	}

	public void setData(Data data) {
		Data = data;
	}
	
	
	private void fromJsonNode(JsonNode inputObj){
		if(inputObj.has("idrequest"))
			this.idrequest = Integer.parseInt(inputObj.get("idrequest").asText());
		else
			this.idrequest = -1;
		this.Host = inputObj.get("host").textValue();
		this.Type = Integer.parseInt(inputObj.get("type").asText());
		this.Command = Integer.parseInt(inputObj.get("command").asText());
		this.rc = Integer.parseInt(inputObj.get("rc").asText());
		if(inputObj.has("output"))
			this.Output = inputObj.get("output");
		if(inputObj.has("data"))
			this.Data = new Data(inputObj.get("data"));
		if(inputObj.has("filename"))
			this.filename = inputObj.get("filename").textValue();
		if(inputObj.has("backup"))
			this.Backup = inputObj.get("backup");
	}

	@Override
    public String toString() {
        return "Value{" +
                "host='" + Host +
                "', destinationHost='" + destinationhost + 
                "', type='" + Type +
                "', idRequest='" + idrequest +
                "', returnCode='" + rc +
                "', Command='" + Command +
                "', Output='" + Output +
                "', Data='" + Data +
                "', fileName='" + filename +
                "', Backup='" + Backup +'\'' +
                '}';
    }

}
