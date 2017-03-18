package it.cnr.isti.pad.fs.storage;

import java.io.IOException;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;
import it.cnr.isti.pad.fs.entry.App;
import voldemort.versioning.VectorClock;

public class Data {

	private Integer idFile;
	private boolean isReplica;
	private String author;
	private String owner;
	private String fileName;
	private String pathToFile;
	private VectorClock version;
	private String file;
	private ArrayList<Data> conflict;


	/**
	 * Data constructor. Creates new Data object from given input
	 * 
	 * @param idFile : node's internal id of the file
	 * @param isReplica : true if this file is a replica's copy
	 * @param author : the name of the author of this file
	 * @param owner : node owner of this file 
	 * @param fileName : the name of the file
	 * @param pathToFile : real path of the file
	 * @param version : timestamp of creation/modification
	 */
	public Data(Integer idFile, boolean isReplica, String author, String owner, String fileName, String pathToFile, String file, VectorClock version){
		this.idFile = idFile;
		this.isReplica = isReplica;
		this.author = author;
		this.owner = owner;
		this.fileName = fileName;
		this.pathToFile = pathToFile;
		this.version = version;
		this.file = file;
		this.conflict = new ArrayList<Data>();
	}

	/**
	 * Data constructor. Creates new Data object from the given JSONObject extracted from a StorageMessage.
	 * 
	 * @param inputDataFromMessage : the Data field of a StorageMessage
	 * @throws JSONException : if an error occurs while parsing JSON
	 * @throws IOException : if an error occurs while writing/reading Data locally
	 */
	public Data(JSONObject inputDataFromMessage) throws JSONException{
		this.conflict = new ArrayList<Data>();
		this.fromJSONObject(inputDataFromMessage);
	}
	/**
	 * Function getConflicts. Returns the array of other version of this Data.
	 * @return ArrayList<Data> containing all the version of this file.
	 */
	public ArrayList<Data> getConflicts(){
		return conflict;
	}
	
	/**
	 * Function addConflict. Add a new version of this Data which is in conflict with the actual version.
	 * @param dataInConflict version of this Data in conflict.
	 */
	public void addConflict(Data dataInConflict){
		this.conflict.add(dataInConflict);
	}
	
	/**
	 * Function hasConflict. Returns true or false depending on the presence of conflicts.
	 * @return true if conflicts exists, false otherwise.
	 */
	public boolean hasConflict(){
		return !(this.conflict.size() == 0);
	}
	
	/**
	 * Function clearConflict. Clear the array of conflicts after a conflict resolution.
	 */
	public void clearConflict(){
		this.conflict.clear();
	}

	/**
	 * Function getOwner. Retrieve the owner's name of this file.
	 * @return the name of the owner
	 */
	public String getOwner() {
		return owner;
	}

	/**
	 * Function setOwner. Set the owner's name to the given owner param.
	 * @param owner the name of the owner to be saved to the file.
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * Function getFile. Retrieve the file contained in this Data object.
	 * @return the file encoded in base64.
	 */
	public String getFile() {
		return file;
	}

	/** 
	 * Function setFile. Set the given file in this Data object. The given file must be encoded in base64.
	 * @param file base64 encoded file.
	 */
	public void setFile(String file) {
		this.file = file;
	}

	/**
	 * Function getIdFile. Retrieve the unique id of this file.
	 * @return the id of this file.
	 */
	public Integer getIdFile() {
		return idFile;
	}
	
	/** 
	 * Function setIdFile. Set the given id to this Data object.
	 * @param idFile integer representing the id of this file.
	 */
	public void setIdFile(Integer idFile) {
		this.idFile = idFile;
	}
	
	/**
	 * Function isReplica. Returns true or false depending on the fact that this Data is a replica or not.
	 * @return true if this file is a replica, false otherwise.
	 */
	public boolean isReplica() {
		return isReplica;
	}
	
	/**
	 * Function setReplica. Set isReplica value for this file.
	 * @param isReplica boolean value to be set.
	 */
	public void setReplica(boolean isReplica) {
		this.isReplica = isReplica;
	}
	
	/**
	 * Function getAuthor. Retrieve the name of the author of this file.
	 * @return the name of the author of this file.
	 */
	public String getAuthor() {
		return author;
	}
	
	/**
	 * Function setAuthor. Set author name for this Data object.
	 * @param author the name of the author for this file.	
	 */
	public void setAuthor(String author) {
		this.author = author;
	}
	
	/**
	 * Function getFileName. Retrieve the name of the file contained into this Data object.
	 * @return the name of the file in this Data object.
	 */
	public String getFileName() {
		return fileName;
	}
	
	/**
	 * Function setFileName. Set the filename of the file contained in this Data object.
	 * @param fileName the name of the file to be set in this Data object.
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	/**
	 * Function getPathToFile. Retrieve the full path in which the file is stored.
	 * @return the real path in which the file in this Data object is stored.
	 */
	public String getPathToFile() {
		return pathToFile;
	}
	
	/**
	 * Function setPathToFile. Set the full path in which the file is stored.
	 * @param pathToFile the real path to be set in this Data object.
	 */
	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}
	
	/**
	 * Function getVersion. Retrieve the actual version of this Data object.
	 * @return VectoClock representing the actual version of this Data object.
	 */
	public VectorClock getVersion() {
		return version;
	}
	
	/**
	 * Function setVersion. Set the new version for this Data object.
	 * @param vc VectorClock containing the new version to be set.
	 */
	public void setVersion(VectorClock vc) {
		this.version = vc;
	}

	public JSONObject toJSONObject(){
		JSONObject obj = new JSONObject();
		try {
			obj.put(Fields.idFile, this.idFile);
			obj.put(Fields.isReplica, this.isReplica);
			obj.put(Fields.author, this.author);
			obj.put(Fields.fileName, this.fileName);
			obj.put(Fields.pathToFile, this.pathToFile);
			if(this.version != null)
				obj.put(Fields.version, this.version.toJSONObject());
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
			if(this.version != null)
				obj.put(Fields.version, this.version.toJSONObject());
			if(this.file != null)
				obj.put(Fields.file, this.file);
			else
				throw new IOException("The given file does not exists.");
		} catch (JSONException | IOException e) {
			e.printStackTrace();
			App.LOGGER.error("Error while serializing Data object.");
			return null;
		}
		return obj;
	}

	private void fromJSONObject(JSONObject inputJson) throws JSONException{
		if(inputJson.has(Fields.idFile))
			this.idFile = inputJson.getInt(Fields.idFile);
		else
			this.idFile = -1;
		this.isReplica = inputJson.getBoolean(Fields.isReplica);
		this.author = inputJson.getString(Fields.author);
		this.fileName = inputJson.getString(Fields.fileName);
		this.pathToFile = inputJson.getString(Fields.pathToFile);
		if(inputJson.has(Fields.version))
			this.version = new VectorClock(inputJson.getJSONObject(Fields.version));
		else
			this.version = null;
		if(inputJson.has(Fields.conflict)){
			for(Object dataOfConflict : inputJson.getJSONArray(Fields.conflict)){
				this.conflict.add(new Data((JSONObject) dataOfConflict));
			}
		}
		if(inputJson.has(Fields.file))
			this.file = inputJson.getString(Fields.file);
	}


	public static class Fields{
		public static String idFile = "idFile";
		public static String isReplica = "isReplica";
		public static String author = "author";
		public static String fileName = "fileName";
		public static String pathToFile = "filepath";
		public static String version = "version";
		public static String file = "file";
		public static String conflict = "conflict";
	}

}
