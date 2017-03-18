package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.code.gossip.GossipMember;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;
import com.google.common.io.Files;
import com.sun.mail.iap.ByteArray;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import it.cnr.isti.hpclab.consistent.ConsistentHasher.HashFunction;
import it.cnr.isti.hpclab.consistent.ConsistentHasher.SHA1HashFunction;
import it.cnr.isti.pad.fs.udpsocket.Message;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageReceiverThreadImpl;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageSenderThreadImpl;
import voldemort.versioning.VectorClock;
import it.cnr.isti.pad.fs.event.OnMessageReceivedListener;
import it.cnr.isti.pad.fs.event.OnResponseHandlerFinished;
import it.cnr.isti.pad.fs.gossiping.GossipResourceService;
import it.cnr.isti.pad.fs.runnables.StorageResponseAsyncHandler;

/** 
 * StorageNode class. 
 * It implements the logic of the storage functions of each node of the net.
 * It also implement the GossipListener interface in order to listen for new UP/DOWN nodes.
 * 
 * @author Andrea Tesei
 * */
public class StorageNode extends Thread implements GossipListener, OnMessageReceivedListener, OnResponseHandlerFinished {

	public static AtomicInteger localMembersIDCounter = new AtomicInteger(0);

	public static AtomicInteger requestIDCounter = new AtomicInteger(0);

	// The files for which this node is responsible.
	public static ConcurrentHashMap<String, Data> myFiles = null;

	// The backup files for which this node acts as replica.
	public static ConcurrentHashMap<String, HashMap<String,Data>> backupFiles = null;

	// The Consistent Hasher instance
	public static ConsistentHasher<String, String> cHasher = null;

	// HashMap of my pending requests
	public static ConcurrentHashMap<Integer, StorageMessage> pendingRequest = null;

	// HashMap of my responses for pending requests
	public static ConcurrentHashMap<Integer, StorageMessage> pendingResponse = null;

	// Instance of Gossip Service
	public static GossipResourceService grs = null;

	// My personal ip
	public static String myHost = null;

	// My personal id
	public static String myId = null;

	public static final Logger LOGGER = Logger.getLogger(StorageNode.class);

	private static AtomicBoolean keepRunning;
	
	public static StorageNodeUtils utils = null;

	// Runnable for sending messages to other nodes
	public static StorageSenderThreadImpl senderThread = null;

	// Runnable-daemon for receiving messages from other nodes
	public static StorageReceiverThreadImpl receiverThread = null;

	public static StorageResponseAsyncHandler responseHandlerThread = null;

	// Executor service used to run threads
	public static ExecutorService executor = null;

	public final static HashFunction Hash_SHA1 = new SHA1HashFunction();

	private char[] workchars = {'|', '/', '-', '\\'};

	/**
	 * StorageNode constructor. 
	 * 
	 * @param fileSettings the file containing the settings for this StorageNode.
	 * @throws UnknownHostException 
	 * @throws SocketException
	 */
	public StorageNode(File fileSettings) throws UnknownHostException, SocketException{
		myFiles = new ConcurrentHashMap<String, Data>();
		backupFiles = new ConcurrentHashMap<String, HashMap<String, Data>>();
		pendingRequest = new ConcurrentHashMap<Integer, StorageMessage>();
		pendingResponse = new ConcurrentHashMap<Integer, StorageMessage>();
		grs = new GossipResourceService(fileSettings, this);
		executor = Executors.newCachedThreadPool();
		utils = new StorageNodeUtils();
		keepRunning = new AtomicBoolean(true);
		int i = 0;
		while(grs.getGossipService().get_gossipManager().getMemberList().isEmpty()){
			System.out.printf("\r%s%c", "Please wait for server initialization...", workchars[i]);
			try {
				sleep(250);
			} catch (InterruptedException e) {
			}
			i++;
			if(i == workchars.length-1)
				i = 0;
		}
		System.out.printf("\r%s\n", "Please wait for server initialization...done.");
		cHasher = new ConsistentHasherImpl<>(
				this.logBase2(grs.getGossipService().get_gossipManager().getMemberList().size(), 2),
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.SHA1);
		senderThread = new StorageSenderThreadImpl();
		receiverThread = new StorageReceiverThreadImpl();
		responseHandlerThread = new StorageResponseAsyncHandler();
		responseHandlerThread.addListener(this);
		receiverThread.addListener(this);
		myHost = StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost();
		myId = grs.getGossipService().get_gossipManager().getMyself().getId();
		cHasher.addBucket(myId);
		grs.getGossipService().get_gossipManager().getMemberList().forEach(member -> cHasher.addBucket(member.getId()));
	}


	@Override
	public void run() {
		Scanner reader = new Scanner(System.in);  // Reading from System.in
		File fileSavedBefore = new File("./files/storagedata.json");
		StorageNode.executor.execute(receiverThread);
		if(fileSavedBefore.exists()){
			try {
				byte[] baForSaveBeforeFiles = Files.toByteArray(fileSavedBefore);
				JSONArray jsonArray = new JSONArray(new String(baForSaveBeforeFiles));
				jsonArray.forEach(file -> 
				{
					Data dataFile;
					try {
						dataFile = new Data((JSONObject)file);
						cHasher.addMember(dataFile.getFileName());
						StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
					} catch (Exception e) {
						StorageNode.LOGGER.error("An error occurred while parsing previous saved file. JSON = " + file.toString());
					}
				});
				fileSavedBefore.delete();
			} catch (IOException e3) {
				StorageNode.LOGGER.error("An error occurs while reading files from local directory. Path=./files/storagedata.json. Error = " + e3.getMessage());
			}
		}
		JSONArray backup = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> backup.put(dataFile.toJSONObjectWithFile()));
		int i = 0;
		int attempts = 0;
		if(!StorageNode.myFiles.isEmpty()){
			// Send now previous saved files to current responsable and put backup for the remaining files
			// For each file you check if you are the responsable bucket or not: 
			// 		If yes, you have to send as PUT_BACKUP to your next node
			// 		else, you have to send it to the relative node;
			List<ByteBuffer> myVirtualBuckets = cHasher.getAllVirtualBucketsFor(myId);
			List<String> names = new ArrayList<>();
			Enumeration<String> fileNamesIterator = StorageNode.myFiles.keys();
			while(fileNamesIterator.hasMoreElements())
				names.add(fileNamesIterator.nextElement());
			ArrayList<Integer> ids = new ArrayList<>();
			for(String fileName : names){
				List<String> possibleBucketFor = utils.retrieveBucketForMember(fileName);
				if(!possibleBucketFor.isEmpty() && !possibleBucketFor.get(0).equals(myId)){
					int idUpdateRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage putRequest = new StorageMessage(StorageNode.myHost,
							utils.getIPFromID(possibleBucketFor.get(0)),
							Message.Type.REQUEST, 
							idUpdateRequest, 
							Message.Command.PUT, 
							-1, 
							null, 
							fileName, 
							myFiles.get(fileName), 
							null);
					cHasher.removeMember(fileName);
					StorageNode.pendingRequest.put(idUpdateRequest, putRequest);
					StorageNode.senderThread.addSendRequestToQueue(putRequest, utils.getIPFromID(possibleBucketFor.get(0)));
					ids.add(idUpdateRequest);
					//	The file will be fisically removed on response
				}
			}
			for(ByteBuffer virtBucket : myVirtualBuckets)
			{
				// Create PUT_BACKUP messages for each descendant of this virtBucket
				List<String> possibleBackup = cHasher.getMembersForVirtualBucket(myId, virtBucket, names);
				if(!possibleBackup.isEmpty()){
					String ipCurrentReplica = utils.getIPFromID(cHasher.getDescendantBucketKey(myId, virtBucket));
					int idBackupRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage putBackup = new StorageMessage(StorageNode.myHost,
							ipCurrentReplica,
							Message.Type.REQUEST, 
							idBackupRequest, 
							Message.Command.PUT_BACKUP, 
							-1, 
							null, 
							null, 
							null, 
							backup);
					StorageNode.pendingRequest.put(idBackupRequest, putBackup);
					StorageNode.senderThread.addSendRequestToQueue(putBackup, ipCurrentReplica);
					ids.add(idBackupRequest);
				}
			}
			if(!ids.isEmpty()){
				StorageNode.responseHandlerThread.addIdsToHandle(ids);
				StorageNode.executor.execute(StorageNode.senderThread);
				StorageNode.executor.execute(StorageNode.responseHandlerThread);
			}



		} 
		System.out.println("Welcome back!");
		System.out.println("This is your Administrator's command-line interface of the distributed file system:");
		while (keepRunning.get()) {
			System.out.println("type one of these commands: ClUSTER_STATUS, NODE2FILEMAP, QUIT or HELP for additional explanation.");
			// manage input from user + send message and process requests
			String input = reader.nextLine();
			List<String> commandAndArgs = null;
			if(input.contains(";"))
				commandAndArgs = Arrays.asList(input.split(";"));
			else {
				commandAndArgs = new ArrayList<String>();
				commandAndArgs.add(input);
			}
			switch(commandAndArgs.get(0)){
			case "CLUSTER_STATUS":
				// Print all nodes with active/down flag
				System.out.println("Cluster status up to now:");
				System.out.println(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost() + " (this) state UP");
				StorageNode.grs.getGossipService().get_gossipManager().getMemberList().forEach(alive -> System.out.println(alive.getHost() + " state UP"));
				StorageNode.grs.getGossipService().get_gossipManager().getDeadList().forEach(dead -> System.out.println(dead.getHost() + " state DOWN"));
				break;
			case "NODE2FILEMAP":
				// Print all file for each node
				ArrayList<Integer> idsListRequest = new ArrayList<Integer>();
				for(LocalGossipMember node : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
					int currListIdRequest = StorageNode.requestIDCounter.getAndIncrement();
					idsListRequest.add(currListIdRequest);
					StorageMessage askForFileList = new StorageMessage(StorageNode.myHost,
							node.getHost(),
							Message.Type.REQUEST,
							currListIdRequest,
							Message.Command.LIST,
							-1,
							null,
							null,
							null,
							null);
					StorageNode.pendingRequest.put(currListIdRequest, askForFileList);
					StorageNode.senderThread.addSendRequestToQueue(askForFileList, node.getHost());
				}
				StorageNode.executor.execute(StorageNode.senderThread);
				i = 0;
				attempts = 0;
				while(StorageNode.pendingRequest.size() > 0 && attempts < 20){
					System.out.printf("\r%s%c", "Please wait while retrieving file list from other nodes...", workchars[i]);
					try {
						sleep(250);
					} catch (InterruptedException e) {
					}
					i++;
					attempts++;
					if(i == workchars.length-1)
						i = 0;
				}
				System.out.printf("\r%s\n", "Please wait while retrieving file list from other nodes...done.");
				HashMap<String, ArrayList<String>> fileList = new HashMap<>();
				ArrayList<String> buffer1 = new ArrayList<>();
				myFiles.forEach((name, file) -> buffer1.add(name));
				fileList.put(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), buffer1);
				// Reading incoming messages for list files
				idsListRequest.forEach(idRequest -> 
				{
					StorageMessage listResponse = StorageNode.pendingResponse.remove(idRequest);
					if(listResponse != null){
						if(listResponse.getReturnCode() != Message.ReturnCode.ERROR){
							JSONArray receivedListFiles = listResponse.getOutput();
							if(!receivedListFiles.getJSONObject(0).has("status")){
								ArrayList<String> buffer2 = new ArrayList<>();
								for(int j = 0; j < receivedListFiles.length(); j++){
									Data newDataFromRemoteNode = null;
									try {
										newDataFromRemoteNode = new Data(receivedListFiles.getJSONObject(j));
										buffer2.add(newDataFromRemoteNode.getFileName());
										if(j+1 == receivedListFiles.length()){
											fileList.put(listResponse.getHost(), buffer2);
										}
									} catch (Exception e) {
										StorageNode.LOGGER.error("Error while parsing LIST response. Please try again. Error = " + e.getMessage());
									}
								}
							}
						} else
							StorageNode.LOGGER.warn("The list file command encountered a problem while receiving file's list from host = " + listResponse.getHost());
					} else {
						System.err.println("One remote host did not receive the LIST request. Please retry.");
					}
				});
				fileList.forEach((ip,nameList) -> 
				{
					System.out.println("Node @ " + ip + " stores:");
					if(!nameList.isEmpty()){
						nameList.forEach(file -> System.out.println(file));
						System.out.println("");
					} else
						System.out.println("No files.\n");
				});
				break;
			case "HELP":
				System.out.println("This is the help message of PADfs administrative cmd interface.\n\n"
						+ "The active features of this version provides the following methods:\n\n"
						+ "\t-CLUSTER_STATUS: returns all the current state of each node of the cluster;\n\n"
						+ "\t-NODE2FILEMAP: returns all the mappings between nodes and files;\n\n"
						+ "\t-HELP: to print this help;\n\n"
						+ "\t-QUIT: last but not least, the command for quitting this Storage instance.\n");
				break;
			case "QUIT":
				System.out.println("Quitting...");
				System.exit(0);
				break;
			default:
				System.out.println(input + ": Command not recognized.");
			}
		}
	}

	public void shutdown() throws IOException{
		File fileForSaveData = new File("./files/storagedata.json");
		JSONArray allData = new JSONArray();
		for(Data file : StorageNode.myFiles.values()) {
			allData.put(file.toJSONObjectWithFile());
		}
		Files.write(allData.toString().getBytes(), fileForSaveData);
		StorageNode.receiverThread.shutdown();
		StorageNode.senderThread.shutdown();
		StorageNode.executor.shutdown();
		try {
			boolean result = StorageNode.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
			if (!result){
				LOGGER.error("executor shutdown timed out");
			}
		} catch (InterruptedException e) {
			LOGGER.error(e);
		}
		keepRunning.set(false);
	}

	// Auxiliary function: calculate the logarithm of the given "input" with the given "base"
	public int logBase2(int input, int base){
		return (int) (Math.log(input) / Math.log(base));
	}

	@Override
	public void onReceivedMessage(StorageMessage receivedMsg) {
		try {
			utils.processRequest(receivedMsg);
		} catch (Exception e) {
			e.printStackTrace();
			StorageNode.LOGGER.warn("The system isn't able to process a received message. It will be dropped: " + receivedMsg.toJSONObject().toString() + " . Error = " + e.getMessage());
		}
	}

	@Override
	public void gossipEvent(GossipMember member, GossipState state) {
		// if member == ipReplica =>
		// 						If the state is down: send member's data to the new copy_replica and cleanup the env
		// if member == ipForWhichIActAsReplica =>
		//						If state is down: you has to pass all your backup data in myfiles and DO NOT change isReplica=true: this information could be useful
		// if member == ? => 
		//						If the state is down: cancel from consistentHash and erase all pending request to it.
		//						If the state is up: check if it is your new replica (ERASE_BACKUP + PUT_BACKUP); check if it is your new master (BACKUP_BACK)
		try {
			// Generic node case
			if(state == GossipState.DOWN){
				System.out.println("Dead Bucket: " + member.toJSONObject());
				// Check if some virtual node is the previous of this dead bucket and save it.
				ArrayList<ByteBuffer> virtBucketForUpdateReplica = new ArrayList<>();
				StorageNode.cHasher.getAllVirtualBucketsFor(myId).forEach(virtBucket -> 
				{
					if(StorageNode.cHasher.getDescendantBucketKey(myId, virtBucket).equals(member.getId())){
						virtBucketForUpdateReplica.add(virtBucket);
					}
				});
				StorageNode.cHasher.removeBucket(member.getId());
				// drop all pending request directed to this node
				StorageNode.pendingRequest.forEach((idRequest, message) -> 
				{ 
					if(message.getHost().equals(member.getHost())){
						StorageNode.pendingRequest.remove(idRequest);
						if(StorageResponseAsyncHandler.idsToHandle.contains(idRequest))
							StorageResponseAsyncHandler.idsToHandle.remove(idRequest);
					}
				});
				// Check whenever you have some files for which you have to act as backup copies
				ArrayList<Integer> ids = new ArrayList<>();
				virtBucketForUpdateReplica.forEach(virtBucket -> 
				{
					// Send backup files to new replica node.
					JSONArray backup = new JSONArray();
					ArrayList<String> members = new ArrayList<>();
					myFiles.keySet().forEach(filename -> members.add(filename));
					String newBucketReplica = cHasher.getDescendantBucketKey(myId, virtBucket);
					List<String> fileToSendForBackup = cHasher.getMembersForNewBackupBucket(myId, newBucketReplica, members);
					fileToSendForBackup.forEach(fileName -> backup.put(myFiles.get(fileName).toJSONObjectWithFile()));
					if(!fileToSendForBackup.isEmpty()){
						if(newBucketReplica != null){
							System.out.println("PUT_BACKUP " + fileToSendForBackup + " in " + utils.getIPFromID(newBucketReplica));
							int idRequestForPutBackup = StorageNode.requestIDCounter.getAndIncrement();
							StorageMessage messageForNewPutBackup = new StorageMessage(StorageNode.myHost,
									utils.getIPFromID(newBucketReplica),
									Message.Type.REQUEST,
									idRequestForPutBackup,
									Message.Command.PUT_BACKUP,
									-1,
									null,
									null,
									null,
									backup);
							StorageNode.pendingRequest.put(idRequestForPutBackup, messageForNewPutBackup);
							StorageNode.senderThread.addSendRequestToQueue(messageForNewPutBackup, utils.getIPFromID(newBucketReplica));
							ids.add(idRequestForPutBackup);
						} else {
							// You are the only one in the network
							StorageNode.LOGGER.warn("No more node replica for " + fileToSendForBackup + ". Maybe this node is the only one in the network.");
						}
					}
				});
				if(StorageNode.backupFiles.containsKey(member.getId())){
					HashMap<String, Data> backupFromDeadMember = StorageNode.backupFiles.get(member.getId()); 

					backupFromDeadMember.forEach((filename, data) -> 
					{
						ArrayList<ByteBuffer> virtualBucketFor = utils.retrieveVirtualBucketForMember(data.getFileName());
						if(!virtualBucketFor.isEmpty()){
							int idRequestUpdate;
							try {
								idRequestUpdate = utils.putData(data);
								if(idRequestUpdate != -1 && idRequestUpdate != -2)
									ids.add(idRequestUpdate);
							} catch (Exception e) {
								StorageNode.LOGGER.error("The system encountered an error while saving the new file " + filename + ". Please take care of this situation. Error = " + e.getMessage());
							}
						} else {
							StorageNode.LOGGER.warn("The file " + filename + " has been saved in this node but apparently all other nodes are down.");
							try {
								utils.putData(data);
							} catch (Exception e) {
								StorageNode.LOGGER.error("The system encountered an error while saving the new file " + filename + ". Please take care of this situation. Error = " + e.getMessage());
							}
						}
					});
					utils.deleteBackupDatas(member.getId());
				}
				if(!ids.isEmpty()){
					StorageNode.responseHandlerThread.addIdsToHandle(ids);
					StorageNode.executor.execute(StorageNode.senderThread);
					StorageNode.executor.execute(StorageNode.responseHandlerThread);
				}
			} else {
				System.out.println("NEW Bucket: " + member.toJSONObject());
				// Check if this node has some data to send to new node.
				StorageNode.cHasher.addBucket(member.getId());
				ArrayList<Data> setDataForSendUpdate = new ArrayList<Data>();
				StorageNode.myFiles.forEach((filename, data) -> 
				{
					ArrayList<String> bucketFor = utils.retrieveBucketForMember(filename);
					if(bucketFor.get(0).equals(member.getId())){ 
						setDataForSendUpdate.add(data);
					}
				});
				ArrayList<Integer> ids = new ArrayList<>();
				// Send all your datas which are property of the new node.
				setDataForSendUpdate.forEach(data -> 
				{
					int idForUpdate = StorageNode.requestIDCounter.getAndIncrement();
					int idForDeleteBackup = StorageNode.requestIDCounter.getAndIncrement();
					ids.add(idForUpdate);
					ids.add(idForDeleteBackup);
					ArrayList<ByteBuffer> virtualBucketFor = utils.retrieveVirtualBucketForMember(data.getFileName());
					if(!virtualBucketFor.isEmpty()){
						String ipCurrentReplica = utils.getIPFromID(cHasher.getDescendantBucketKey(myId, virtualBucketFor.get(0)));
						StorageMessage updateFile = new StorageMessage(
								StorageNode.myHost,
								member.getHost(),
								Message.Type.REQUEST,
								idForUpdate,
								Message.Command.PUT,
								-1,
								null,
								data.getFileName(),
								data,
								null);
						// Delete here on cHasher and fisically on Response
						cHasher.removeMember(data.getFileName());
						StorageMessage deleteBackupFile = new StorageMessage(
								StorageNode.myHost,
								ipCurrentReplica,
								Message.Type.REQUEST,
								idForDeleteBackup,
								Message.Command.DELETE_BACKUP,
								-1,
								null,
								data.getFileName(),
								null,
								null);
						StorageNode.senderThread.addSendRequestToQueue(updateFile, member.getHost());
						StorageNode.senderThread.addSendRequestToQueue(deleteBackupFile, ipCurrentReplica);
						StorageNode.pendingRequest.put(idForDeleteBackup, deleteBackupFile);
						StorageNode.pendingRequest.put(idForUpdate, updateFile);
					}
				});
				if(!ids.isEmpty()){
					StorageNode.responseHandlerThread.addIdsToHandle(ids);
					StorageNode.executor.execute(StorageNode.senderThread);
					StorageNode.executor.execute(StorageNode.responseHandlerThread);
				}


				// List<ByteBuffer> allMyVirtBuckets = cHasher.getAllVirtualBucketsFor(myId);
				JSONArray backup = new JSONArray();
				ArrayList<String> members = new ArrayList<>();
				myFiles.keySet().forEach(filename -> members.add(filename));
				List<String> fileToSendForBackup = cHasher.getMembersForNewBackupBucket(myId, member.getId(), members);
				fileToSendForBackup.forEach(fileName -> backup.put(myFiles.get(fileName).toJSONObjectWithFile()));
				if(backup.length() != 0){
					// This is the case whenever new node replica born. PUT_BACKUP in new node and wait result: if ok then cleanup old backup files else RETRY
					// send PUT_BACKUP and be sure it receive the message (with separate thread?)
					int idRequestForPutBackup = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage messageForNewPutBackup = new StorageMessage(StorageNode.myHost,
							utils.getIPFromID(member.getId()),
							Message.Type.REQUEST,
							idRequestForPutBackup,
							Message.Command.PUT_BACKUP,
							-1,
							null,
							null,
							null,
							backup);
					StorageNode.pendingRequest.put(idRequestForPutBackup, messageForNewPutBackup);
					StorageNode.responseHandlerThread.addSingleIdToQueue(idRequestForPutBackup);
					StorageNode.executor.execute(StorageNode.senderThread);
					StorageNode.executor.execute(StorageNode.responseHandlerThread);
				}
			}

		} catch (InterruptedException e) {
			StorageNode.LOGGER.error("An error occurred while deteting bucket: " + member.getHost() + ":" + member.getId() + ". Error = " + e.getMessage());
		}
	}

	@Override
	public void onFinishedHandleResponse(ArrayList<Integer> ids) {
		if(!ids.isEmpty()){
			// Check whenever some request must be send another time
			ArrayList<Integer> newIds = new ArrayList<>();
			ids.forEach(id -> 
			{
				if(StorageNode.pendingRequest.containsKey(id)){
					if(StorageNode.pendingRequest.get(id).getHandlerCounter() < 2){
						StorageMessage thismessage = StorageNode.pendingRequest.get(id);
						StorageNode.pendingRequest.remove(id);
						int newIdForResend = StorageNode.requestIDCounter.getAndIncrement();
						thismessage.setIdRequest(newIdForResend);
						thismessage.setHandlerCounter(thismessage.getHandlerCounter()+1);
						StorageNode.senderThread.addSendRequestToQueue(thismessage, thismessage.getDestinationHost());
						newIds.add(newIdForResend);
						StorageNode.pendingRequest.put(newIdForResend, thismessage);
					} else
						StorageNode.LOGGER.error("The message with id " + id + " for " + StorageNode.pendingRequest.get(id).getDestinationHost() + " encountered an error during delivery. " + StorageNode.pendingRequest.get(id).toJSONObject());
				} else
					StorageNode.LOGGER.error("The message with id " + id + " does not exists in pending request. This tells that something goes wrong...");
			});
			StorageResponseAsyncHandler.idsToHandle.clear();
			if(!newIds.isEmpty()){
				StorageNode.responseHandlerThread.addIdsToHandle(newIds);
				StorageNode.executor.execute(senderThread);
				StorageNode.executor.execute(responseHandlerThread);
			}
		}
	}

}
