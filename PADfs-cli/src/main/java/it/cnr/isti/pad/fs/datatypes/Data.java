package it.cnr.isti.pad.fs.datatypes;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Data {

	private Integer idFile;
	private boolean isReplica;
	private String author;
	private String owner;
	private String fileName;
	private String filepath;
	private String file;
	private ArrayList<Data> conflict;

	
	public Data(){
		
	}
	
	public Data(JsonNode data){
		this.fromJsonNode(data);
	}
	
	public ArrayList<Data> getConflict(){
		return conflict;
	}
	
	
	public void setConflict(ArrayList<Data> conflict) {
		this.conflict = conflict;
	}

	public void addConflict(Data dataInConflict){
		this.conflict.add(dataInConflict);
	}
	
	public boolean hasConflict(){
		return !(this.conflict.size() == 0);
	}
	
	public void clearConflict(){
		this.conflict.clear();
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
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
	
	public String getFilepath() {
		return filepath;
	}

	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
	
	
	private void fromJsonNode(JsonNode inputJson){
		if(inputJson.has("idFile"))
			this.idFile = Integer.parseInt(inputJson.get("idFile").textValue());
		else
			this.idFile = -1;
		this.isReplica = Boolean.parseBoolean(inputJson.get("isReplica").textValue());
		this.author = inputJson.get("author").textValue();
		this.fileName = inputJson.get("fileName").textValue();
		this.filepath = inputJson.get("filepath").textValue();
		if(inputJson.has("conflict")){
			for(JsonNode dataOfConflict : inputJson.get("conflict")){
				this.conflict.add(new Data(dataOfConflict));
			}
		}
		if(inputJson.has("file"))
			this.file = inputJson.get("file").textValue();
	}


	@Override
    public String toString() {
        return "Data{" +
                "idFile='" + idFile +
                "', isReplica='" + isReplica + 
                "', author='" + author + 
                "', owner='" + owner + 
                "', fileName='" + fileName + 
                "', pathToFile='" + filepath + 
                "', file='" + file + 
                "', conflict='" + conflict + '\'' +
                '}';
    }

}
