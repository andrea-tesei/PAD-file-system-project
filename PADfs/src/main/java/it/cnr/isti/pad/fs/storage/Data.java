package it.cnr.isti.pad.fs.storage;

import voldemort.versioning.VectorClock;

public class Data {
	
	private Integer idFile;
	private boolean isReplica;
	private String author;
	private String fileName;
	private String pathToFile;
	private VectorClock version;
	
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
	public void setVersion(VectorClock version) {
		this.version = version;
	}

}
