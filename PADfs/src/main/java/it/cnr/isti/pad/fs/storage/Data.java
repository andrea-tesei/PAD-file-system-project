package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.io.Files;

import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import voldemort.versioning.VectorClock;

public class Data {
	
	private Integer idFile;
	private boolean isReplica;
	private String author;
	private String fileName;
	private String pathToFile;
	private VectorClock version;
	private File file;
	
	
	/**
	 * Data constructor. Creates new Data object from given input
	 * 
	 * @param idFile : node's internal id of the file
	 * @param isReplica : true if this file is a replica's copy
	 * @param author 
	 * @param fileName
	 * @param pathToFile : real path of the file
	 * @param version : timestamp of creation/modification
	 */
	public Data(Integer idFile, boolean isReplica, String author, String fileName, String pathToFile, VectorClock version){
		this.idFile = idFile;
		this.isReplica = isReplica;
		this.author = author;
		this.fileName = fileName;
		this.pathToFile = pathToFile;
		this.version = version;
		this.file = new File(this.pathToFile + this.fileName);
	}
	
	public Data(JSONObject inputDataFromMessage) throws JSONException, IOException{
		if(inputDataFromMessage.has(StorageMessage.Fields.DATA)){
			if(((JSONObject) inputDataFromMessage.get(StorageMessage.Fields.DATA)).has(Data.Fields.file))
				this.fromJSONObjectWithFile(inputDataFromMessage);
		} else
			this.fromJSONObject(inputDataFromMessage);
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public Integer getIdFile() {
		return idFile;
	}
	public void setIdFile(Integer idFile) {
		this.idFile = idFile;
	}
	public boolean isReplica() {
		return isReplica;
	}
	public void setReplica(boolean isReplica) {
		this.isReplica = isReplica;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getPathToFile() {
		return pathToFile;
	}
	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}
	public VectorClock getVersion() {
		return version;
	}
	public void setVersion(long timestamp) {
		this.version = new VectorClock(timestamp);
	}
	
	public JSONObject toJSONObject(){
		JSONObject obj = new JSONObject();
		try {
			obj.put(Fields.idFile, this.idFile);
			obj.put(Fields.isReplica, this.isReplica);
			obj.put(Fields.author, this.author);
			obj.put(Fields.fileName, this.fileName);
			obj.put(Fields.pathToFile, this.pathToFile);
			obj.put(Fields.version, this.version.getTimestamp());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			App.LOGGER.error("Error while serializing Data object.");
			return null;
		}
		return obj;
	}
	
	public JSONObject toJSONObjectWithFile(){
		JSONObject obj = new JSONObject();
		try {
			obj.put(Fields.idFile, this.idFile);
			obj.put(Fields.isReplica, this.isReplica);
			obj.put(Fields.author, this.author);
			obj.put(Fields.fileName, this.fileName);
			obj.put(Fields.pathToFile, this.pathToFile);
			obj.put(Fields.version, this.version.getTimestamp());
			if(this.file != null){
				if(this.file.isFile())
					obj.put(Fields.file, Base64.encodeBase64String(Files.toByteArray(this.file)));
				else
					throw new IOException("The given file does not exists.");
			}
		} catch (JSONException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			App.LOGGER.error("Error while serializing Data object.");
			return null;
		}
		return obj;
	}
	
	private void fromJSONObject(JSONObject inputJson) throws JSONException{
			this.idFile = inputJson.getInt(Fields.idFile);
			this.isReplica = inputJson.getBoolean(Fields.isReplica);
			this.author = inputJson.getString(Fields.author);
			this.fileName = inputJson.getString(Fields.fileName);
			this.pathToFile = inputJson.getString(Fields.pathToFile);
			this.version = new VectorClock(inputJson.getLong(Fields.version));
	}
	
	private void fromJSONObjectWithFile(JSONObject inputJson) throws JSONException, IOException{
		this.idFile = inputJson.getInt(Fields.idFile);
		this.isReplica = inputJson.getBoolean(Fields.isReplica);
		this.author = inputJson.getString(Fields.author);
		this.fileName = inputJson.getString(Fields.fileName);
		this.pathToFile = inputJson.getString(Fields.pathToFile);
		this.version = new VectorClock(inputJson.getLong(Fields.version));
		byte[] rcvdFile = Base64.decodeBase64(inputJson.getString(Fields.file));
		// save the file to disk
		this.file = new File(this.pathToFile + this.fileName);
		Files.write(rcvdFile, this.file);
	}
	
	public static class Fields{
		public static String idFile = "idFile";
		public static String isReplica = "isReplica";
		public static String author = "author";
		public static String fileName = "fileName";
		public static String pathToFile = "filepath";
		public static String version = "version";
		public static String file = "file";
	}

}
