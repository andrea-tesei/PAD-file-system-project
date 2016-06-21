package it.cnr.isti.pad.fs.storage;

//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//
//import org.apache.commons.codec.binary.Base64;
//import org.json.JSONArray;
//import org.json.JSONObject;
//
//import com.google.code.gossip.LocalGossipMember;
//import com.google.common.io.Files;
//
//import it.cnr.isti.pad.fs.udpsocket.Message;
//import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
//import voldemort.versioning.VectorClock;

public class StorageNodeUtils {

//	public String getIPFromID(String id){
//		String ret = "";
//		for(LocalGossipMember member: StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
//			if(member.getId().equals(id))
//				ret = member.getHost();
//		}
//		return ret;
//	}
//
//	public String getIDFromIP(String ip){
//		String ret = "";
//		for(LocalGossipMember member: StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
//			if(member.getHost().equals(ip))
//				ret = member.getId();
//		}
//		return ret;
//	}
//	
//	// Auxiliary function: calculate the logarithm of the given "input" with the given "base"
//		public int logBase2(int input, int base){
//			return (int) (Math.log(input) / Math.log(base));
//		}
//
//		// Auxiliary function: retrieve the backet's id corresponding to the given member. (if the member isn't in any bucket, the function return it's possible bucket)
//		public static ArrayList<String> retrieveBucketForMember(String member){
//			List<String> memb = new ArrayList<String>();
//			memb.add(member);
//			ArrayList<String> ret = new ArrayList<String>();
//			StorageNode.cHasher.getAllBuckets().forEach(bucket ->
//			{
//				if(!StorageNode.cHasher.getMembersFor(bucket, memb).isEmpty())
//					ret.add(bucket);
//			});
//			return ret;
//		}
//
//		// PutData: the assumption is that this methods is called for update/put
//		//		 only files that belongs to this bucket, otherwise return err.
//		//		 P.S.: take care about replica copies
//		// RETURNS: the id of the request for UPDATE_BACKUP (if the file is new), -1 otherwise
//		private int putData(Data dataFile, String ipReplica) throws IOException{
//			int ret = -1;
//			byte[] rcvdFile = null;
//			File filesDir = null;
//			if(!StorageNode.myFiles.containsKey(dataFile.getFileName())){
//				int idNewFile = StorageNode.localMembersIDCounter.getAndIncrement();
//				dataFile.setIdFile(idNewFile);
//				dataFile.setPathToFile("./files/");
//				dataFile.setReplica(false);
//				StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
//				StorageNode.cHasher.addMember(dataFile.getFileName());
//				// Save file locally
//				rcvdFile = Base64.decodeBase64(dataFile.getFile());
//				// save the file to disk
//				filesDir = new File("./files");
//				if(!filesDir.exists()){
//					filesDir.mkdir();
//				}
//				File fileToSave = new File("./files/" + dataFile.getFileName());
//				Files.write(rcvdFile, fileToSave);
//				if(!ipReplica.equals("")){
//					int currentIDREQ = StorageNode.requestIDCounter.getAndIncrement();
//					// Send file to replica node
//					StorageMessage sendBackup = new StorageMessage(StorageNode.myHost,
//							ipReplica,
//							Message.Type.REQUEST, 
//							currentIDREQ, 
//							Message.Command.UPDATE_BACKUP, 
//							-1, 
//							null, 
//							dataFile.getFileName(), 
//							StorageNode.myFiles.get(dataFile.getFileName()), 
//							null);
//					StorageNode.pendingRequest.put(currentIDREQ, sendBackup);
//					StorageNode.senderThread.addSendRequestToQueue(sendBackup, ipReplica);
//					ret = currentIDREQ;
//				}
//			} else {
//				// Update the given file here and in replica node
//				for(String key : StorageNode.myFiles.keySet()){
//					if(StorageNode.myFiles.get(key).getFileName().equals(dataFile.getFileName())){
//						VectorClock myCopy = StorageNode.myFiles.get(key).getVersion();
//						VectorClock newCopy = dataFile.getVersion();
//						ArrayList<Integer> ids = new ArrayList<>();
//						switch (myCopy.compare(newCopy)) {
//						case AFTER:
//							// Do nothing
//							break;
//						case BEFORE:
//							// Update value and send update to replica node
//							dataFile.setIdFile(StorageNode.myFiles.get(key).getIdFile());
//							StorageNode.myFiles.remove(key);
//							StorageNode.myFiles.put(key, dataFile);
//							rcvdFile = Base64.decodeBase64(dataFile.getFile());
//							// save the file to disk
//							filesDir = new File(dataFile.getPathToFile());
//							if(!filesDir.exists()){
//								filesDir.mkdir();
//							}
//							File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
//							Files.write(rcvdFile, fileToSave);
//							if(!ipReplica.equals("")){
//								int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
//								// Send updated value to backup copy
//								StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
//										ipReplica,
//										Message.Type.REQUEST,
//										idRequestForUpdateBackup,
//										Message.Command.UPDATE_BACKUP,
//										-1,
//										null,
//										dataFile.getFileName(),
//										StorageNode.myFiles.get(dataFile.getFileName()),
//										null);
//								ret = idRequestForUpdateBackup;
//								ids.add(ret);
//								this.responseHandlerThread.addIdsToHandle(ids);
//								StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
//								this.senderThread.addSendRequestToQueue(updateBackup, ipReplica);
//							}
//							break;
//						case CONCURRENTLY:
//							System.out.println("CONFLICT: myCopy=" + myCopy.getTimestamp() + " newCopy=" + newCopy.getTimestamp());
//							Data copyOfConflict = StorageNode.myFiles.get(key);
//							copyOfConflict.addConflict(dataFile);
//							StorageNode.myFiles.remove(key);
//							StorageNode.myFiles.put(dataFile.getFileName(), copyOfConflict);
//							int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
//							// Send updated value to backup copy
//							StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
//									ipReplica,
//									Message.Type.REQUEST,
//									idRequestForUpdateBackup,
//									Message.Command.UPDATE_BACKUP,
//									-1,
//									null,
//									dataFile.getFileName(),
//									StorageNode.myFiles.get(dataFile.getFileName()),
//									null);
//							ret = idRequestForUpdateBackup;
//							ids.add(ret);
//							this.responseHandlerThread.addIdsToHandle(ids);
//							StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
//							this.senderThread.addSendRequestToQueue(updateBackup, ipReplica);
//							break;
//						}
//					}
//				}
//			}
//			return ret;
//		}
//
//
//		private int putBackupData(String idMaster, ArrayList<Data> backup) throws IOException{
//			int ret = 0;
//			System.out.println("Put backup for: " + idMaster + "  " + this.getIDFromIP(idMaster));
//			if(!StorageNode.backupFiles.containsKey(idMaster)){
//				StorageNode.backupFiles.put(idMaster, new HashMap<String, Data>());
//			}
//			for(Data data : backup){
//				int tempRet = updateBackupData(idMaster, data, false);
//				if(tempRet == -1){
//					// CONFLICT
//					ret = tempRet;
//					System.err.println("PUT Backup conflict: " + data.getFileName());
//				}
//			}
//			return ret;
//		}
//
//		/**
//		 * 
//		 * @param file
//		 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
//		 * @throws IOException 
//		 */
//		private int updateBackupData(String idMaster, Data file, boolean force) throws IOException{
//			int ret = 0;
//			if(!StorageNode.backupFiles.get(idMaster).containsKey(file.getFileName())){
//				file.setReplica(true);
//				HashMap<String, Data> currentBackup = StorageNode.backupFiles.get(idMaster); 
//				StorageNode.backupFiles.remove(idMaster);
//				currentBackup.put(file.getFileName(), file);
//				StorageNode.backupFiles.put(idMaster, currentBackup);
//				System.out.println("Backup files updated: " + StorageNode.backupFiles.get(idMaster).size());
//				byte[] rcvdFile = Base64.decodeBase64(file.getFile());
//				// save the file to disk
//				File backupDir = new File("./backup");
//				if(!backupDir.exists()){
//					backupDir.mkdir();				
//				}
//				File fileToSave = new File("./backup/"+file.getFileName());
//				Files.write(rcvdFile, fileToSave);
//			} else {
//				Data oldCopy = StorageNode.backupFiles.get(idMaster).get(file.getFileName());
//				VectorClock myBackupVector = oldCopy.getVersion();
//				VectorClock fileVector = file.getVersion();
//				if(!force){
//					HashMap<String, Data> currentBackupFiles = null;
//					switch (myBackupVector.compare(fileVector)) {
//					case AFTER:
//						// Do nothing
//						break;
//					case BEFORE:
//						// Update value 
//						file.setIdFile(oldCopy.getIdFile());
//						currentBackupFiles = StorageNode.backupFiles.remove(idMaster);
//						currentBackupFiles.put(file.getFileName(), file);
//						StorageNode.backupFiles.put(idMaster, currentBackupFiles);
//						ret = 1;
//						break;
//					case CONCURRENTLY:
//						file.setIdFile(oldCopy.getIdFile());
//						file.addConflict(oldCopy);
//						currentBackupFiles = StorageNode.backupFiles.remove(idMaster);
//						currentBackupFiles.put(file.getFileName(), file);
//						StorageNode.backupFiles.put(idMaster, currentBackupFiles);
//						ret = -1;
//						break;
//					}
//				} else {
//					file.setIdFile(oldCopy.getIdFile());
//					HashMap<String, Data> currentBackupFiles = StorageNode.backupFiles.remove(idMaster);
//					currentBackupFiles.put(file.getFileName(), file);
//					StorageNode.backupFiles.put(idMaster, currentBackupFiles);
//					ret = 1;
//				}
//					
//			}
//			return ret;
//		}
//
//		private int deleteData(String fileName, String ipReplica) throws IOException{
//			int ret = -1;
//			// Check if this file already exists
//			Boolean trovato = false;
//			// Check if the file is already 
//			for(String member : StorageNode.cHasher.getAllMembers()){
//				if(member.equals(fileName))
//					trovato = true;
//			}
//			if(trovato == false){
//				// The requested file does not exists
//				StorageNode.LOGGER.warn("The file " + fileName + " does not exists and so cannot be deleted.");
//			} else {
//				StorageNode.cHasher.removeMember(fileName);
//				Data deletedData = StorageNode.myFiles.remove(fileName);
//				File fileToDelete = new File(deletedData.getPathToFile()  + deletedData.getFileName());
//				if(fileToDelete.delete()){
//					int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
//					StorageMessage requestForDeleteBackup = new StorageMessage(StorageNode.myHost,
//							ipReplica,
//							Message.Type.REQUEST,
//							currDeleteIdRequest,
//							Message.Command.DELETE_BACKUP,
//							-1,
//							null,
//							fileName,
//							null,
//							null);
//					ret = currDeleteIdRequest;
//					StorageNode.pendingRequest.put(currDeleteIdRequest, requestForDeleteBackup);
//					this.senderThread.addSendRequestToQueue(requestForDeleteBackup, ipReplica);
//				} else
//					throw new IOException("An error occurs while deleting file: " + deletedData.getPathToFile() + "/" + deletedData.getFileName());
//			}
//			return ret;
//		}
//
//		private JSONArray retrieveJSONListFiles(){
//			JSONArray output = new JSONArray();
//			StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile.toJSONObject()));
//			return output;
//		}
//
//		private int resolveConflictResolution(Data dataToUpdate, String senderIP, String ipReplica){
//			int ret = 0;
//			byte[] rcvdFile = null;
//			File filesDir = null;
//			try {
//				if(StorageNode.myFiles.containsKey(dataToUpdate.getFileName())){
//					dataToUpdate.setIdFile(myFiles.get(dataToUpdate.getFileName()).getIdFile());
//					StorageNode.myFiles.remove(dataToUpdate.getFileName());
//					StorageNode.myFiles.put(dataToUpdate.getFileName(), dataToUpdate);
//					rcvdFile = Base64.decodeBase64(dataToUpdate.getFile());
//					// save the file to disk
//					filesDir = new File(dataToUpdate.getPathToFile());
//					if(!filesDir.exists()){
//						filesDir.mkdir();
//					}
//					File fileToSave = new File(dataToUpdate.getPathToFile() + dataToUpdate.getFileName());
//					Files.write(rcvdFile, fileToSave);
//					if(!ipReplica.equals("")){
//						int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
//						// Send updated value to backup copy
//						StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
//								ipReplica,
//								Message.Type.REQUEST,
//								idRequestForUpdateBackup,
//								Message.Command.CONFLICT_RESOLUTION,
//								-1,
//								null,
//								dataToUpdate.getFileName(),
//								StorageNode.myFiles.get(dataToUpdate.getFileName()),
//								null);
//						ret = idRequestForUpdateBackup;
//						StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
//						this.senderThread.addSendRequestToQueue(updateBackup, ipReplica);
//					}
//				} else if(StorageNode.backupFiles.containsKey(this.getIDFromIP(senderIP))){
//					if(StorageNode.backupFiles.get(this.getIDFromIP(senderIP)).containsKey(dataToUpdate.getFileName()))
//						this.updateBackupData(this.getIDFromIP(senderIP), dataToUpdate, true);
//				} else
//					StorageNode.LOGGER.warn("The given request for CONFLICT_RESOLUTION is wrong: no " + dataToUpdate.getFileName() + " file saved locally.");
//			} catch (IOException e) {
//				e.printStackTrace();
//				StorageNode.LOGGER.error("An error occurred when saving file in local directory. " + dataToUpdate.getFileName());
//				ret = -1;
//			}
//			return ret;
//		}

}
