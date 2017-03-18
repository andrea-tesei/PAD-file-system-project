package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.code.gossip.LocalGossipMember;
import com.google.common.io.Files;

import it.cnr.isti.pad.fs.udpsocket.Message;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import voldemort.versioning.VectorClock;

/**
 * StorageNodeUtils class.
 * It implements several auxiliary function used by StorageNode instance.
 * 
 * @author Andrea Tesei
 *
 */
public class StorageNodeUtils {
	
		public StorageNodeUtils(){ }
	
		/**
		 * Function retrieveBucketForMember. Retrieve the backet's ids corresponding to the given file name.
		 * If the given file isn't in any bucket, the function return it's "possible" bucket.
		 * 
		 * @param member the file name to searching for.
		 * @return the array of possible bucket's id.
		 */
		// Auxiliary function: retrieve the backet's id corresponding to the given member. (if the member isn't in any bucket, the function return it's possible bucket)
		public ArrayList<String> retrieveBucketForMember(String member){
			List<String> memb = new ArrayList<String>();
			memb.add(member);
			ArrayList<String> ret = new ArrayList<String>();
			StorageNode.cHasher.getAllBuckets().forEach(bucket ->
			{
				if(!StorageNode.cHasher.getMembersFor(bucket, memb).isEmpty())
					ret.add(bucket);
			});
			return ret;
		}

		/**
		 * Function retrieveVirtualBucketForMember. Retrieve the virtual backet's ids corresponding to the given file name.
		 * 
		 * @param member the file name to searching for.
		 * @return the array of possible virtual buckets.
		 */
		public ArrayList<ByteBuffer> retrieveVirtualBucketForMember(String member){
			ArrayList<String> memb = new ArrayList<String>();
			memb.add(member);
			ArrayList<ByteBuffer> selectedKeys = new ArrayList<>();
			StorageNode.cHasher.getAllBuckets().forEach(bucketName -> 
			{
				List<ByteBuffer> virtualNodes = StorageNode.cHasher.getAllVirtualBucketsFor(bucketName);
				virtualNodes.forEach(virtNode ->
				{
					if(!StorageNode.cHasher.getMembersForVirtualBucket(bucketName, virtNode, memb).isEmpty()){
						selectedKeys.add(virtNode);
					}
				});
			});
			return selectedKeys;
		}

		/**
		 * Function putData. This method performs the UPDATE/PUT operation for a given Data object.
		 * Note that this method perform this operation only for files that belongs to this node, otherwise return error.
		 * 
		 * @param dataFile the Data object to be stored
		 * @return an integer representing the id of the data replica's store/update request, 0 if no action is performed (in case the currently stored version is updated),
		 * -1 if the file is null and -2 if this node is the only one in the cluster.
		 * 
		 * @throws IOException
		 */
		public int putData(Data dataFile) throws IOException{
			int ret = 0;
			byte[] rcvdFile = null;
			File filesDir = null;
			dataFile.setPathToFile("./files/");
			ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(dataFile.getFileName());
			String ipCurrentReplica = null;
			if(virtualBucketFor.isEmpty()){
				// this node is the last one in the network. Save the file without replica and print a message.
				StorageNode.LOGGER.warn("The system at this point is composed only by this node. From now, consistency and availability are not guaranteed.");
				ret = -2;
			} else
				ipCurrentReplica = this.getIPFromID(StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId, virtualBucketFor.get(0)));
			int idRequestForUpdateBackup;
			StorageMessage updateBackup = null;
			if(!StorageNode.myFiles.containsKey(dataFile.getFileName())){
				System.out.println("Myfiles does not contains " + dataFile.getFileName());
				if(dataFile.getVersion() == null){
					ByteBuffer myIdAsHash = ByteBuffer.wrap(StorageNode.Hash_SHA1.hash(StorageNode.myId.getBytes()));
					VectorClock newVC = new VectorClock();
					newVC.incrementVersion(Math.abs((short) myIdAsHash.getInt()), System.currentTimeMillis());
					dataFile.setVersion(newVC);
				}
				if(dataFile.getFile() == null){
					System.out.println("The given file is null. Put aborted.");
					ret = -1;
				} else {
					int idNewFile = StorageNode.localMembersIDCounter.getAndIncrement();
					dataFile.setIdFile(idNewFile);
					dataFile.setPathToFile("./files/");
					dataFile.setReplica(false);
					StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
					StorageNode.cHasher.addMember(dataFile.getFileName());
					// Save file locally
					rcvdFile = Base64.decodeBase64(dataFile.getFile());
					// save the file to disk
					filesDir = new File("./files");
					if(!filesDir.exists()){
						filesDir.mkdir();
					}
					File fileToSave = new File("./files/" + dataFile.getFileName());
					Files.write(rcvdFile, fileToSave);
					if(ipCurrentReplica != null){
						int currentIDREQ = StorageNode.requestIDCounter.getAndIncrement();
						// Send file to replica node
						StorageMessage sendBackup = new StorageMessage(StorageNode.myHost,
								ipCurrentReplica,
								Message.Type.REQUEST, 
								currentIDREQ, 
								Message.Command.UPDATE_BACKUP, 
								-1, 
								null, 
								dataFile.getFileName(), 
								StorageNode.myFiles.get(dataFile.getFileName()), 
								null);
						StorageNode.responseHandlerThread.addSingleIdToQueue(currentIDREQ);
						StorageNode.pendingRequest.put(currentIDREQ, sendBackup);
						StorageNode.senderThread.addSendRequestToQueue(sendBackup, ipCurrentReplica);
						ret = currentIDREQ;
					}
				}
			} else {
				System.out.println("Myfiles contains " + dataFile.getFileName());
				ArrayList<Integer> ids = new ArrayList<>();
				if(dataFile.getVersion() != null){
					VectorClock myCopy = StorageNode.myFiles.get(dataFile.getFileName()).getVersion();
					VectorClock newCopy = dataFile.getVersion();
					switch (myCopy.compare(newCopy)) {
					case AFTER:
						System.out.println("my vector clock is AFTER NEW CLOCK");
						ret = 0;
						// Do nothing
						break;
					case BEFORE:
						System.out.println("my vector clock is BEFORE new clock");
						// Update value and send update to replica node
						dataFile.setIdFile(StorageNode.myFiles.get(dataFile.getFileName()).getIdFile());
						Data oldCopy = StorageNode.myFiles.remove(dataFile.getFileName());
						dataFile.setVersion(dataFile.getVersion().merge(oldCopy.getVersion()));
						StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
						rcvdFile = Base64.decodeBase64(dataFile.getFile());
						// save the file to disk
						filesDir = new File(dataFile.getPathToFile());
						if(!filesDir.exists()){
							filesDir.mkdir();
						}
						File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
						Files.write(rcvdFile, fileToSave);
						if(ipCurrentReplica != null){
							idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
							// Send updated value to backup copy
							updateBackup = new StorageMessage(StorageNode.myHost, 
									ipCurrentReplica,
									Message.Type.REQUEST,
									idRequestForUpdateBackup,
									Message.Command.UPDATE_BACKUP,
									-1,
									null,
									dataFile.getFileName(),
									StorageNode.myFiles.get(dataFile.getFileName()),
									null);
							ret = idRequestForUpdateBackup;
							ids.add(ret);
							StorageNode.responseHandlerThread.addIdsToHandle(ids);
							StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
							StorageNode.senderThread.addSendRequestToQueue(updateBackup, ipCurrentReplica);
						}
						break;
					case CONCURRENTLY:
						Data copyOfConflict = StorageNode.myFiles.get(dataFile.getFileName());
						copyOfConflict.addConflict(dataFile);
						copyOfConflict.addConflict(copyOfConflict);
						StorageNode.myFiles.remove(dataFile.getFileName());
						StorageNode.myFiles.put(dataFile.getFileName(), copyOfConflict);
						if(dataFile.getFile().equals(copyOfConflict.getFile()))
							// if the two files are equal, merge and do nothing
							System.out.println("The two given files have the same contents. Filename = " + dataFile.getFileName());
						// Send updated value to backup copy
						if(StorageNode.myFiles.get(dataFile.getFileName()).hasConflict()){
							System.out.println("The given file has conflicts:");
							for(Data conflict : StorageNode.myFiles.get(dataFile.getFileName()).getConflicts()){
								System.out.println("Version=" + conflict.getVersion().toJSONObject());
							}
						} else
							System.out.println("No conflict ");
						if(ipCurrentReplica != null){
							idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
							updateBackup = new StorageMessage(StorageNode.myHost, 
									ipCurrentReplica,
									Message.Type.REQUEST,
									idRequestForUpdateBackup,
									Message.Command.UPDATE_BACKUP,
									-1,
									null,
									dataFile.getFileName(),
									StorageNode.myFiles.get(dataFile.getFileName()),
									null);
							ret = idRequestForUpdateBackup;
							ids.add(ret);
							StorageNode.responseHandlerThread.addIdsToHandle(ids);
							StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
							StorageNode.senderThread.addSendRequestToQueue(updateBackup, ipCurrentReplica);
						}
						break;
					}
				} else {
					// The given PUT is really an update from client of the given file => increment_version + override
					dataFile.setIdFile(StorageNode.myFiles.get(dataFile.getFileName()).getIdFile());
					Data oldCopy = StorageNode.myFiles.remove(dataFile.getFileName());
					ByteBuffer myIdAsHash = ByteBuffer.wrap(StorageNode.Hash_SHA1.hash(StorageNode.myId.getBytes()));
					dataFile.setVersion(oldCopy.getVersion().incremented(Math.abs((short) myIdAsHash.getInt()), System.currentTimeMillis()));
					StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
					rcvdFile = Base64.decodeBase64(dataFile.getFile());
					// save the file to disk
					filesDir = new File(dataFile.getPathToFile());
					if(!filesDir.exists()){
						filesDir.mkdir();
					}
					File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
					Files.write(rcvdFile, fileToSave);
					if(ipCurrentReplica != null){
						idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
						// Send updated value to backup copy
						updateBackup = new StorageMessage(StorageNode.myHost, 
								ipCurrentReplica,
								Message.Type.REQUEST,
								idRequestForUpdateBackup,
								Message.Command.UPDATE_BACKUP,
								-1,
								null,
								dataFile.getFileName(),
								StorageNode.myFiles.get(dataFile.getFileName()),
								null);
						ret = idRequestForUpdateBackup;
						ids.add(ret);
						StorageNode.responseHandlerThread.addIdsToHandle(ids);
						StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
						StorageNode.senderThread.addSendRequestToQueue(updateBackup, ipCurrentReplica);
					}
				}
			}
			return ret;
		}
		/**
		 * Function updateBackupData. Performs the action of update backup for the given Data object.
		 * 
		 * @param idMaster the id of the master node for this file.
		 * @param file the Data object to be updated in the backup node.
		 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
		 * @throws IOException 
		 */
		private void updateBackupData(String idMaster, Data file) throws IOException{
			file.setPathToFile("./backup/");
			if(!StorageNode.backupFiles.get(idMaster).containsKey(file.getFileName())){
				file.setReplica(true);
				HashMap<String, Data> currentBackup = StorageNode.backupFiles.get(idMaster); 
				StorageNode.backupFiles.remove(idMaster);
				currentBackup.put(file.getFileName(), file);
				StorageNode.backupFiles.put(idMaster, currentBackup);
			} else { 
				Data oldCopy = StorageNode.backupFiles.get(idMaster).get(file.getFileName());
				file.setIdFile(oldCopy.getIdFile());
				HashMap<String, Data> currentBackupFiles = StorageNode.backupFiles.remove(idMaster);
				currentBackupFiles.put(file.getFileName(), file);
				StorageNode.backupFiles.put(idMaster, currentBackupFiles);
			}
			byte[] rcvdFile = Base64.decodeBase64(file.getFile());
			// save the file to disk
			File backupDir = new File("./backup");
			if(!backupDir.exists()){
				backupDir.mkdir();				
			}
			File fileToSave = new File("./backup/"+file.getFileName());
			Files.write(rcvdFile, fileToSave);
		}
		
		/**
		 * Function putBackupData. Performs the action of put/update backup for the given set of Data objects.
		 * 
		 * @param idMaster the id of the master node for this set of files.
		 * @param backup the list of Data object to be send to the replica's node.
		 * @return either 0 in case of success, -1 in case of problem with a file backup.
		 * @throws IOException
		 */
		private int putBackupData(String idMaster, ArrayList<Data> backup){
			int ret = 0;
			if(!StorageNode.backupFiles.containsKey(idMaster)){
				StorageNode.backupFiles.put(idMaster, new HashMap<String, Data>());
			}
			for(Data data : backup){
				try {
					updateBackupData(idMaster, data);
				} catch (IOException e) {
					e.printStackTrace();
					StorageNode.LOGGER.error("Error while put backup data for " + idMaster + ".");
					return -1;
				}
			}
			return ret;
		}

		/**
		 * Function deleteData. Performs operation of deleting the given file name if exists.
		 * 
		 * @param fileName the file name to be deleted.
		 * @return either the id of the request for deletion in the backup node in case of success, 
		 * -1 in case of error while deleting data, -2 in case this is the last node in the cluster.
		 * @throws IOException
		 */
		public int deleteData(String fileName) throws IOException{
			int ret = -1;
			// Check if the file exists
			if(!StorageNode.cHasher.getAllMembers().contains(fileName)){
				// The requested file does not exists
				StorageNode.LOGGER.warn("The file " + fileName + " does not exists and so cannot be deleted.");
			} else {
				ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(fileName);
				if(!virtualBucketFor.isEmpty()){
					String ipCurrentReplica = this.getIPFromID(StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId, virtualBucketFor.get(0)));
					StorageNode.cHasher.removeMember(fileName);
					Data deletedData = StorageNode.myFiles.remove(fileName);
					File fileToDelete = new File(deletedData.getPathToFile()  + deletedData.getFileName());
					if(fileToDelete.delete()){
						int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
						StorageMessage requestForDeleteBackup = new StorageMessage(StorageNode.myHost,
								ipCurrentReplica,
								Message.Type.REQUEST,
								currDeleteIdRequest,
								Message.Command.DELETE_BACKUP,
								-1,
								null,
								fileName,
								null,
								null);
						ret = currDeleteIdRequest;
						StorageNode.responseHandlerThread.addSingleIdToQueue(currDeleteIdRequest);
						StorageNode.pendingRequest.put(currDeleteIdRequest, requestForDeleteBackup);
						StorageNode.senderThread.addSendRequestToQueue(requestForDeleteBackup, ipCurrentReplica);
					} else 
						throw new IOException("An error occurs while deleting file: " + deletedData.getPathToFile() + "/" + deletedData.getFileName());
				} else {
					StorageNode.LOGGER.warn("This node seem to be the last one in the network. From now, consistency and availability are not guaranteed.");
					ret = -2;
				}
			}
			return ret;
		}

		/**
		 * Function deleteBackupDatas. Performs the delete operation for the backup data in the given backup node's id.
		 * 
		 * @param idBackupBucket id of the node in which delete all backup data.
		 */
		public void deleteBackupDatas(String idBackupBucket){
			// Check if the file exists
			if(StorageNode.backupFiles.containsKey(idBackupBucket)){
				HashMap<String, Data> backupToRemove = StorageNode.backupFiles.remove(idBackupBucket);
				backupToRemove.forEach((filename, file) -> 
				{
					System.out.println("Cleanup backup file: " + "./backup/" + file.getFileName());
					File fileToDelete = new File("./backup/"  + file.getFileName());
					fileToDelete.delete();
				});
			} else 
				StorageNode.LOGGER.warn("The given id is not listed as node for which i'm replica.");
		}

		private void cleanupOldBackup(){
			List<ByteBuffer> myVirtualNodes = StorageNode.cHasher.getAllVirtualBucketsFor(StorageNode.myId);
			List<String> myDescendantBucket = new ArrayList<>();
			for(ByteBuffer virtBucket : myVirtualNodes)
				myDescendantBucket.add(StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId, virtBucket));
			Enumeration<String> backupIterator = StorageNode.backupFiles.keys();
			List<String> removedBackupForBucket = new ArrayList<>();
			while(backupIterator.hasMoreElements()){
				String backupBucket = backupIterator.nextElement();
				if(!myDescendantBucket.contains(backupBucket)){
					this.deleteBackupDatas(backupBucket);
					removedBackupForBucket.add(backupBucket);
				}
			}
			if(!removedBackupForBucket.isEmpty())
				StorageNode.LOGGER.info("Old useless backup deleted: " + removedBackupForBucket);
		}

		/**
		 * Function retrieveJSONListFiles. Retrieve all the files stored in this node.
		 *  
		 * @return the array of the files stored in this node (all Data object as JSONObject)
		 */
		private JSONArray retrieveJSONListFiles(){
			JSONArray output = new JSONArray();
			StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile.toJSONObject()));
			if(output.length() == 0)
				output.put(new JSONObject().put("status", "empty list"));
			return output;
		}

		/**
		 * Function resolveConflictResolution. Performs the resolution of conflicts with the given file.
		 * 
		 * @param tsChosen timestamp choosen by the user.
		 * @param fileName the file name of the file envolved in the resolution operation.
		 * @return either the id of the request for backup copy's update in case of success, 
		 * -1 in case an error occurs while resolve conflict, -2 in case this node is the last one in the network 
		 */
		public int resolveConflictResolution(Long tsChosen, String fileName) {
			int ret = 0;
			byte[] rcvdFile = null;
			File filesDir = null;
			try {
				if(StorageNode.myFiles.containsKey(fileName)){
					VectorClock oldClock = StorageNode.myFiles.get(fileName).getVersion();
					ArrayList<Data> conflicts = StorageNode.myFiles.get(fileName).getConflicts();
					Data dataChosen = null;
					for(Data conflictData : conflicts){
						if(conflictData.getVersion().getTimestamp() == tsChosen){
							dataChosen = conflictData;
						}
					}
					if(dataChosen != null){
						dataChosen.setIdFile(StorageNode.myFiles.get(fileName).getIdFile());
						dataChosen.clearConflict();
						VectorClock newUpdatedClock = oldClock.merge(dataChosen.getVersion());
						dataChosen.setVersion(newUpdatedClock);
						StorageNode.myFiles.remove(fileName);
						StorageNode.myFiles.put(fileName, dataChosen);
						int idrequestForConflict = StorageNode.requestIDCounter.getAndIncrement();
						ArrayList<ByteBuffer> virtualBucketFor = this.retrieveVirtualBucketForMember(fileName);
						// Check if virtualBucketFor is empty: if it is, you have not backup to update
						if(!virtualBucketFor.isEmpty()){
							String ipCurrentReplica = this.getIPFromID(StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId, virtualBucketFor.get(0)));
							StorageMessage msgForConflictResolution = new StorageMessage(StorageNode.myHost,
									ipCurrentReplica,
									Message.Type.REQUEST,
									idrequestForConflict,
									Message.Command.UPDATE_BACKUP,
									-1,
									null,
									fileName,
									dataChosen,
									null);
							ret = idrequestForConflict;
							StorageNode.responseHandlerThread.addSingleIdToQueue(ret);
							StorageNode.pendingRequest.put(idrequestForConflict, msgForConflictResolution);
							StorageNode.senderThread.addSendRequestToQueue(msgForConflictResolution, ipCurrentReplica);
						} else {
							StorageNode.LOGGER.warn("This node seems to be the last one in the network. From now, consistency and availability are not guaranteed.");
							ret = -2;
						}
						rcvdFile = Base64.decodeBase64(dataChosen.getFile());
						// save the file to disk
						filesDir = new File("./backup/");
						if(!filesDir.exists()){
							filesDir.mkdir();
						}
						File fileToSave = new File("./backup/" + fileName);
						Files.write(rcvdFile, fileToSave);
					} else {
						StorageNode.LOGGER.warn("The given request for CONFLICT_RESOLUTION is wrong: no timestamp equal to " + tsChosen + " stored in conflicts list.");
						ret = -1;
					}

				} else {
					StorageNode.LOGGER.warn("The given request for CONFLICT_RESOLUTION is wrong: no " + fileName + " file saved locally.");
					ret = -1;
				}
			} catch (IOException e) {
				StorageNode.LOGGER.error("An error occurred when saving file in local directory. " + fileName + ". Error = " + e.getMessage());
				ret = -1;
			}
			return ret;
		}

		/**
		 * Function processRequest. This method performs the process operation for a received message from other nodes.
		 * 
		 * @param msg the received message
		 * @throws IOException
		 */
		public void processRequest(StorageMessage msg) throws IOException {
			int type = msg.getType();
			int command = msg.getCommand();
			switch(command){
			case Message.Command.GET:
				if(type == Message.Type.REQUEST){
					// check if data is in my bucket
					JSONArray output = new JSONArray();
					output.put(new JSONObject().put((StorageNode.myFiles.containsKey(msg.getFileName())) ? "status" : "error", 
							(StorageNode.myFiles.containsKey(msg.getFileName())) ? "ok" : "The requested file does not exists. " + msg.toJSONObject().toString()));
					Data dataForGet = StorageNode.myFiles.get(msg.getFileName());
					// Check if there aren't conflicts

					StorageMessage response = null;
					if(!output.getJSONObject(0).has("error")){
						if(dataForGet.hasConflict()){
							JSONArray conflictOutput = new JSONArray();
							dataForGet.getConflicts().forEach(data -> conflictOutput.put(data.getVersion().toJSONObject()));
							response = new StorageMessage(StorageNode.myHost,
									msg.getHost(),
									Message.Type.RESPONSE, 
									msg.getIdRequest(), 
									msg.getCommand(), 
									Message.ReturnCode.CONFLICTS_EXISTS, 
									conflictOutput, 
									msg.getFileName(), 
									null, 
									null);
						} else
							response = new StorageMessage(StorageNode.myHost,
									msg.getHost(),
									Message.Type.RESPONSE, 
									msg.getIdRequest(), 
									msg.getCommand(), 
									Message.ReturnCode.OK, 
									output, 
									msg.getFileName(), 
									dataForGet, 
									null);
					} else
						response = new StorageMessage(StorageNode.myHost,
								msg.getHost(),
								Message.Type.RESPONSE, 
								msg.getIdRequest(), 
								msg.getCommand(), 
								Message.ReturnCode.NOT_EXISTS, 
								output, 
								msg.getFileName(), 
								null, 
								null);

					StorageNode.senderThread.addSendRequestToQueue(response, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.PUT:
				if(type == Message.Type.REQUEST){
					int ret = this.putData(msg.getData());
					JSONArray output = new JSONArray();
					if(ret == -1)
						output.put(new JSONObject().put("error", "The given file is null."));
					else if (ret == -2)
						output.put(new JSONObject().put("status", "This node is the only one in the system. From now, consistency and availability features are not guarranteed."));
					else
						output.put(new JSONObject().put("status", "ok"));
					StorageMessage response = new StorageMessage(StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE, 
							msg.getIdRequest(),
							msg.getCommand(),
							(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
									output, 
									msg.getFileName(), 
									null, 
									null);
					StorageNode.senderThread.addSendRequestToQueue(response, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
					StorageNode.executor.execute(StorageNode.responseHandlerThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						// Check if the return code is OK: if not, print an error
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise when store " + msg.getFileName() + " file in " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The file " + msg.getFileName() + " was stored in " + msg.getHost() + " host successfully.");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.DELETE:
				if(type == Message.Type.REQUEST){
					int ret = this.deleteData(msg.getFileName());
					JSONArray output = new JSONArray();
					if(ret == -1)
						output.put(new JSONObject().put("error", "The system encountered a problem while deleting this file."));
					else if(ret == -2)
						output.put(new JSONObject().put("status", "The node " + StorageNode.myHost + " seems to be the last one in the network. From now consistency and availability are not guaranteed."));
					else
						output.put(new JSONObject().put("status", "ok"));
					StorageMessage response = new StorageMessage(StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE, 
							msg.getIdRequest(), 
							msg.getCommand(),
							(output.getJSONObject(0).has("error")) ? Message.ReturnCode.NOT_EXISTS : Message.ReturnCode.OK, 
									output, 
									msg.getFileName(), 
									null,
									null);
					StorageNode.senderThread.addSendRequestToQueue(response, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise while deleting " + msg.getFileName() + " file from " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The file " + msg.getFileName() + " was deleted from " + msg.getHost() + " host successfully.");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.LIST:
				if(type == Message.Type.REQUEST){
					JSONArray output = this.retrieveJSONListFiles();
					StorageMessage response = new StorageMessage(StorageNode.myHost, 
							msg.getHost(),
							Message.Type.RESPONSE, 
							msg.getIdRequest(), 
							msg.getCommand(),
							(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
									output, 
									null, 
									null,
									null);
					StorageNode.senderThread.addSendRequestToQueue(response, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;

			case Message.Command.PUT_BACKUP:
				// This message arrives whenever you ask for backup copy. You have to parse all files and store it
				if(type == Message.Type.REQUEST){
					int ret = 0;
					JSONArray backupCopies = msg.getBackup();
					ArrayList<Data> backup = new ArrayList<>();
					StorageMessage putBackupResponse = null;
					for(int i = 0; i < backupCopies.length() && (ret != -1); i++){
						Data backupCopy = new Data(backupCopies.getJSONObject(i));
						backupCopy.setPathToFile("./backup/");
						backup.add(backupCopy);
					}
					ret = this.putBackupData(this.getIDFromIP(msg.getHost()), backup);
					putBackupResponse = new StorageMessage(StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE,
							msg.getIdRequest(),
							msg.getCommand(),
							(ret != -1) ? Message.ReturnCode.OK : Message.ReturnCode.ERROR,
									null,
									null,
									null,
									null);
					StorageNode.senderThread.addSendRequestToQueue(putBackupResponse, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE){
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() == Message.ReturnCode.ERROR){
							StorageNode.LOGGER.warn("A problem arise while send backup copies to " + msg.getHost() + " host.");
						} else { 
							StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully update my backup copy.");
						}
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.DELETE_BACKUP:
				if(type == Message.Type.REQUEST){
					StorageMessage responseForDeleteBackup = new StorageMessage(
							StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE, 
							msg.getIdRequest(),
							Message.Command.DELETE_BACKUP,
							Message.ReturnCode.OK, 
							null, 
							msg.getFileName(), 
							null, 
							null);
					if(StorageNode.backupFiles.containsKey(this.getIDFromIP(msg.getHost()))){
						HashMap<String, Data> dataToDelete = StorageNode.backupFiles.remove(this.getIDFromIP(msg.getHost()));
						for(Data data : dataToDelete.values()) {
							File deleteFile = new File("./backup/" + data.getFileName());
							if(!deleteFile.delete()){
								dataToDelete.remove(data.getFileName());
								StorageNode.backupFiles.put(this.getIDFromIP(msg.getHost()), dataToDelete);
								responseForDeleteBackup = new StorageMessage(StorageNode.myHost, 
										msg.getHost(),
										Message.Type.RESPONSE, 
										msg.getIdRequest(),
										Message.Command.DELETE_BACKUP,
										Message.ReturnCode.ERROR, 
										null, 
										msg.getFileName(), 
										null, 
										null);
							}
						}
					} else {
						responseForDeleteBackup = new StorageMessage(StorageNode.myHost, 
								msg.getHost(),
								Message.Type.RESPONSE, 
								msg.getIdRequest(),
								Message.Command.DELETE_BACKUP,
								Message.ReturnCode.NOT_EXISTS, 
								null, 
								msg.getFileName(), 
								null, 
								null);
					}
					StorageNode.senderThread.addSendRequestToQueue(responseForDeleteBackup, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise while deleting backup copy of " + msg.getFileName() +  " in " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully deleted backup copy of " + msg.getFileName() + ".");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.UPDATE_BACKUP:
				if(type == Message.Type.REQUEST){
					ArrayList<Data> backup = new ArrayList<>();
					backup.add(msg.getData());
					int ret = this.putBackupData(this.getIDFromIP(msg.getHost()), backup);
					JSONArray output = new JSONArray();
					output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while storing replica data. " + msg.toJSONObject().toString()));
					StorageMessage response = new StorageMessage(StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE, 
							msg.getIdRequest(), 
							msg.getCommand(),
							(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
									output, 
									msg.getFileName(), 
									null,
									null);
					StorageNode.senderThread.addSendRequestToQueue(response, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);
				} else if(type == Message.Type.RESPONSE) {
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise while sending backup copy to " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully received backup copy.");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.ERASE_BACKUP:
				if(type == Message.Type.REQUEST){
					this.deleteBackupDatas(this.getIDFromIP(msg.getHost()));
					StorageMessage eraseResponse = new StorageMessage(
							StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE,
							msg.getIdRequest(),
							msg.getCommand(),
							Message.ReturnCode.OK,
							null,
							null,
							null,
							null);
					StorageNode.senderThread.addSendRequestToQueue(eraseResponse, msg.getHost());
					StorageNode.executor.execute(StorageNode.senderThread);		
				} else if(type == Message.Type.RESPONSE){
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise while erase remote backup copy on " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully erased backup copy.");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			case Message.Command.CONFLICT_RESOLUTION:
				if(type == Message.Type.REQUEST){
					// Parse received message and resolve conflict
					int idRequestForUpdateBackup = this.resolveConflictResolution(msg.getOutput().getLong(0), msg.getFileName());
					StorageMessage conflictResponse = null;
					if(idRequestForUpdateBackup == -1){
						// An error occurred while saving file in local directory
						conflictResponse = new StorageMessage(
								StorageNode.myHost,
								msg.getHost(),
								Message.Type.RESPONSE,
								msg.getIdRequest(),
								msg.getCommand(),
								Message.ReturnCode.ERROR,
								null,
								msg.getFileName(),
								null,
								null);
						StorageNode.senderThread.addSendRequestToQueue(conflictResponse, msg.getHost());
						StorageNode.executor.execute(StorageNode.senderThread);
					} else if(idRequestForUpdateBackup == -2) {
						JSONArray output = new JSONArray();
						output.put(new JSONObject().put("status", "The node " + StorageNode.myHost + " seems to be the last one in the network. From now, consistency and availability are not guaranteed."));
						conflictResponse = new StorageMessage(
								StorageNode.myHost,
								msg.getHost(),
								Message.Type.RESPONSE,
								msg.getIdRequest(),
								msg.getCommand(),
								Message.ReturnCode.OK,
								output,
								msg.getFileName(),
								null,
								null);
						StorageNode.senderThread.addSendRequestToQueue(conflictResponse, msg.getHost());
						StorageNode.executor.execute(StorageNode.senderThread);
					} else if (idRequestForUpdateBackup != 0){
						// Wait result with external thread for response handling
						conflictResponse = new StorageMessage(
								StorageNode.myHost,
								msg.getHost(),
								Message.Type.RESPONSE,
								msg.getIdRequest(),
								msg.getCommand(),
								Message.ReturnCode.OK,
								null,
								msg.getFileName(),
								null,
								null);
						StorageNode.senderThread.addSendRequestToQueue(conflictResponse, msg.getHost());
						StorageNode.executor.execute(StorageNode.senderThread);
						StorageNode.executor.execute(StorageNode.responseHandlerThread);
					}
				} else if(type == Message.Type.RESPONSE){
					if(StorageNode.pendingRequest.containsKey(msg.getIdRequest())){
						StorageNode.pendingRequest.remove(msg.getIdRequest());
						StorageNode.pendingResponse.put(msg.getIdRequest(), msg);
						if(msg.getReturnCode() != Message.ReturnCode.OK)
							StorageNode.LOGGER.warn("A problem arise while resolving conflict for file " + msg.getFileName() + " in remote " + msg.getHost() + " host.");
						else
							StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully update copy of file in conflict.");
					} else 
						// drop the packet
						StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
				}
				break;
			}
		}

	public String getIPFromID(String id){
		String ret = "";
		for(LocalGossipMember member: StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId().equals(id))
				ret = member.getHost();
		}
		return ret;
	}

	public String getIDFromIP(String ip){
		String ret = "";
		for(LocalGossipMember member: StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getHost().equals(ip))
				ret = member.getId();
		}
		return ret;
	}

	/**
	 * Function addRequestToQueue. Add the given message in the queue of the sender thread.
	 * 
	 * @param msg the message to be send.
	 * @param ipAddress the ip address of the destination node.
	 */
	public void addRequestToQueue(StorageMessage msg, String ipAddress){
		StorageNode.senderThread.addSendRequestToQueue(msg, ipAddress);
	}

	/**
	 * Function addResponseToHandlerQueue. Add the given id to the list of response asynchronous handler thread. (For put/update backup data)
	 * 
	 * @param id the unique id of the request.
	 */
	public void addResponseToHandlerQueue(Integer id){
		StorageNode.responseHandlerThread.addSingleIdToQueue(id);
	}

	public void executeSenderThread(){
		StorageNode.executor.execute(StorageNode.senderThread);
	}

	public void executeResponseHandlerThread(){
		StorageNode.executor.execute(StorageNode.responseHandlerThread);
	}

}
