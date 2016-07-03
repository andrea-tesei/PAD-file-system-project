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
 * StorageNode class: it implements the logic of the storage functions of each node of the net.
 * 					  It also implement the GossipListener interface in order to listen for
 * 					  new UP/DOWN nodes.
 * */
// TODO: confict resolution => ok (to be tested) + Delete all e.printstacktrace() and put the associated message to logger => TBD + refactoring => TO be finished + api => ok
// 		For refactoring: this thread reamin for manage command to the nodes: tail -f log, cluster status ecc + use spring boot 
public class StorageNode extends Thread implements GossipListener, OnMessageReceivedListener, OnResponseHandlerFinished {

	public static AtomicInteger localMembersIDCounter = new AtomicInteger(0);

	public static AtomicInteger requestIDCounter = new AtomicInteger(0);

	// The files for which this node is responsible.
	public static ConcurrentHashMap<String, Data> myFiles;

	// The backup files for which this node acts as replica.
	public static ConcurrentHashMap<String, HashMap<String,Data>> backupFiles;

	// The Consistent Hasher instance
	public static ConsistentHasher<String, String> cHasher;

	// HashMap of my pending requests
	public static ConcurrentHashMap<Integer, StorageMessage> pendingRequest;

	// HashMap of my responses for pending requests
	public static ConcurrentHashMap<Integer, StorageMessage> pendingResponse;

	// Instance of Gossip Service
	public static GossipResourceService grs = null;

	// My personal ip
	public static String myHost = null;

	// My personal id
	public static String myId = null;

	public static final Logger LOGGER = Logger.getLogger(StorageNode.class);

	private static AtomicBoolean keepRunning;

	// Runnable for sending messages to other nodes
	private static StorageSenderThreadImpl senderThread = null;

	// Runnable-daemon for receiving messages from other nodes
	private static StorageReceiverThreadImpl receiverThread = null;

	private static StorageResponseAsyncHandler responseHandlerThread = null;

	// Executor service used to run threads
	private static ExecutorService executor = null;

	public final static HashFunction Hash_SHA1 = new SHA1HashFunction();

	private char[] workchars = {'|', '/', '-', '\\'};

	public StorageNode(File fileSettings) throws UnknownHostException, SocketException{
		myFiles = new ConcurrentHashMap<String, Data>();
		backupFiles = new ConcurrentHashMap<String, HashMap<String, Data>>();
		pendingRequest = new ConcurrentHashMap<Integer, StorageMessage>();
		pendingResponse = new ConcurrentHashMap<Integer, StorageMessage>();
		grs = new GossipResourceService(fileSettings, this);
		executor = Executors.newCachedThreadPool();
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

	public static String getIPFromID(String id){
		String ret = "";
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId().equals(id))
				ret = member.getHost();
		}
		return ret;
	}

	public static String getIDFromIP(String ip){
		String ret = "";
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getHost().equals(ip))
				ret = member.getId();
		}
		return ret;
	}

	public static void addRequestToQueue(StorageMessage msg, String ipAddress){
		senderThread.addSendRequestToQueue(msg, ipAddress);
	}

	public static void addResponseToHandlerQueue(Integer id){
		responseHandlerThread.addSingleIdToQueue(id);
	}

	public static void executeSenderThread(){
		executor.execute(senderThread);
	}

	public static void executeResponseHandlerThread(){
		executor.execute(responseHandlerThread);
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
				StorageNode.LOGGER.error("An error occurs while reading files from local directory. Path=./files/storagedata.json. Error = " + e3.getStackTrace());
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
				List<String> possibleBucketFor = StorageNode.retrieveBucketForMember(fileName);
				if(!possibleBucketFor.isEmpty() && !possibleBucketFor.get(0).equals(myId)){
					int idUpdateRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage putRequest = new StorageMessage(StorageNode.myHost,
							StorageNode.getIPFromID(possibleBucketFor.get(0)),
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
					StorageNode.senderThread.addSendRequestToQueue(putRequest, StorageNode.getIPFromID(possibleBucketFor.get(0)));
					ids.add(idUpdateRequest);
					//	The file will be fisically removed on response
				}
			}
			for(ByteBuffer virtBucket : myVirtualBuckets)
			{
				// Create PUT_BACKUP messages for each descendant of this virtBucket
				List<String> possibleBackup = cHasher.getMembersForVirtualBucket(myId, virtBucket, names);
				if(!possibleBackup.isEmpty()){
					String ipCurrentReplica = StorageNode.getIPFromID(cHasher.getDescendantBucketKey(myId, virtBucket));
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
			if(input.contains(":"))
				commandAndArgs = Arrays.asList(input.split(":"));
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
										StorageNode.LOGGER.error("Error while parsing LIST response. Please try again. Error = " + e.getStackTrace());
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

	// Auxiliary function: retrieve the backet's id corresponding to the given member. (if the member isn't in any bucket, the function return it's possible bucket)
	public static ArrayList<String> retrieveBucketForMember(String member){
		List<String> memb = new ArrayList<String>();
		memb.add(member);
		ArrayList<String> ret = new ArrayList<String>();
		cHasher.getAllBuckets().forEach(bucket ->
		{
			if(!cHasher.getMembersFor(bucket, memb).isEmpty())
				ret.add(bucket);
		});
		return ret;
	}

	public static ArrayList<ByteBuffer> retrieveVirtualBucketForMember(String member){
		ArrayList<String> memb = new ArrayList<String>();
		memb.add(member);
		ArrayList<ByteBuffer> selectedKeys = new ArrayList<>();
		cHasher.getAllBuckets().forEach(bucketName -> 
		{
			List<ByteBuffer> virtualNodes = cHasher.getAllVirtualBucketsFor(bucketName);
			virtualNodes.forEach(virtNode ->
			{
				if(!cHasher.getMembersForVirtualBucket(bucketName, virtNode, memb).isEmpty()){
					selectedKeys.add(virtNode);
				}
			});
		});
		return selectedKeys;
	}

	// PutData: the assumption is that this methods is called for update/put
	//		 only files that belongs to this bucket, otherwise return err.
	//		 P.S.: take care about replica copies
	// RETURNS: the id of the request for UPDATE_BACKUP (if the file is new), -1 otherwise
	public static int putData(Data dataFile) throws IOException{
		int ret = -1;
		byte[] rcvdFile = null;
		File filesDir = null;
		dataFile.setPathToFile("./files/");
		ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(dataFile.getFileName());
		String ipCurrentReplica = StorageNode.getIPFromID(cHasher.getDescendantBucketKey(myId, virtualBucketFor.get(0)));
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
			if(dataFile.getFile() == null)
				System.out.println("The given file is null. Put aborted.");
			else {
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
		} else {
			System.out.println("Myfiles contains " + dataFile.getFileName());
			ArrayList<Integer> ids = new ArrayList<>();
			if(dataFile.getVersion() != null){
				VectorClock myCopy = myFiles.get(dataFile.getFileName()).getVersion();
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
					dataFile.setIdFile(myFiles.get(dataFile.getFileName()).getIdFile());
					Data oldCopy = myFiles.remove(dataFile.getFileName());
					dataFile.setVersion(dataFile.getVersion().merge(oldCopy.getVersion()));
					myFiles.put(dataFile.getFileName(), dataFile);
					rcvdFile = Base64.decodeBase64(dataFile.getFile());
					// save the file to disk
					filesDir = new File(dataFile.getPathToFile());
					if(!filesDir.exists()){
						filesDir.mkdir();
					}
					File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
					Files.write(rcvdFile, fileToSave);
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

					break;
				case CONCURRENTLY:
					Data copyOfConflict = myFiles.get(dataFile.getFileName());
					copyOfConflict.addConflict(dataFile);
					myFiles.remove(dataFile.getFileName());
					myFiles.put(dataFile.getFileName(), copyOfConflict);
					if(dataFile.getFile().equals(copyOfConflict.getFile()))
						// if the two files are equal, merge and do nothing
						System.out.println("The two given files have the same contents. Filename = " + dataFile.getFileName());
					idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
					// Send updated value to backup copy
					if(myFiles.get(dataFile.getFileName()).hasConflict()){
						for(Data conflict : myFiles.get(dataFile.getFileName()).getConflicts()){
							System.out.println("Version=" + conflict.getVersion().toJSONObject());
						}
					} else
						System.out.println("No conflict ");
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
					break;
				}
			} else {
				// The given PUT is really an update from client of the given file => increment_version + override
				dataFile.setIdFile(myFiles.get(dataFile.getFileName()).getIdFile());
				Data oldCopy = myFiles.remove(dataFile.getFileName());
				ByteBuffer myIdAsHash = ByteBuffer.wrap(StorageNode.Hash_SHA1.hash(StorageNode.myId.getBytes()));
				dataFile.setVersion(oldCopy.getVersion().incremented(Math.abs((short) myIdAsHash.getInt()), System.currentTimeMillis()));
				myFiles.put(dataFile.getFileName(), dataFile);
				rcvdFile = Base64.decodeBase64(dataFile.getFile());
				// save the file to disk
				filesDir = new File(dataFile.getPathToFile());
				if(!filesDir.exists()){
					filesDir.mkdir();
				}
				File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
				Files.write(rcvdFile, fileToSave);
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
		return ret;
	}


	private int putBackupData(String idMaster, ArrayList<Data> backup) throws IOException{
		int ret = 0;
		if(!StorageNode.backupFiles.containsKey(idMaster)){
			StorageNode.backupFiles.put(idMaster, new HashMap<String, Data>());
		}
		for(Data data : backup){
			updateBackupData(idMaster, data);
		}
		return ret;
	}

	/**
	 * 
	 * @param file
	 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
	 * @throws IOException 
	 */
	private void updateBackupData(String idMaster, Data file) throws IOException{
		file.setPathToFile("./backup/");
		if(!backupFiles.get(idMaster).containsKey(file.getFileName())){
			file.setReplica(true);
			HashMap<String, Data> currentBackup = StorageNode.backupFiles.get(idMaster); 
			StorageNode.backupFiles.remove(idMaster);
			currentBackup.put(file.getFileName(), file);
			StorageNode.backupFiles.put(idMaster, currentBackup);
		} else { 
			Data oldCopy = StorageNode.backupFiles.get(idMaster).get(file.getFileName());
			file.setIdFile(oldCopy.getIdFile());
			HashMap<String, Data> currentBackupFiles = backupFiles.remove(idMaster);
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

	public static int deleteData(String fileName) throws IOException{
		int ret = -1;
		// Check if the file exists
		if(!cHasher.getAllMembers().contains(fileName)){
			// The requested file does not exists
			StorageNode.LOGGER.warn("The file " + fileName + " does not exists and so cannot be deleted.");
		} else {
			ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(fileName);
			String ipCurrentReplica = StorageNode.getIPFromID(cHasher.getDescendantBucketKey(myId, virtualBucketFor.get(0)));
			cHasher.removeMember(fileName);
			Data deletedData = myFiles.remove(fileName);
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
		}
		return ret;
	}

	private static void deleteBackupDatas(String idBackupBucket){
		// Check if the file exists
		if(backupFiles.containsKey(idBackupBucket)){
			HashMap<String, Data> backupToRemove = backupFiles.remove(idBackupBucket);
			backupToRemove.forEach((filename, file) -> 
			{
				System.out.println("Cleanup backup file: " + "./backup/" + file.getFileName());
				File fileToDelete = new File("./backup/"  + file.getFileName());
				fileToDelete.delete();
			});
		} else 
			StorageNode.LOGGER.warn("The given id is not listed as node for which i'm replica.");
	}

	private static void cleanupOldBackup(){
		List<ByteBuffer> myVirtualNodes = cHasher.getAllVirtualBucketsFor(myId);
		List<String> myDescendantBucket = new ArrayList<>();
		for(ByteBuffer virtBucket : myVirtualNodes)
			myDescendantBucket.add(cHasher.getDescendantBucketKey(myId, virtBucket));
		Enumeration<String> backupIterator = backupFiles.keys();
		List<String> removedBackupForBucket = new ArrayList<>();
		while(backupIterator.hasMoreElements()){
			String backupBucket = backupIterator.nextElement();
			if(!myDescendantBucket.contains(backupBucket)){
				StorageNode.deleteBackupDatas(backupBucket);
				removedBackupForBucket.add(backupBucket);
			}
		}
		if(!removedBackupForBucket.isEmpty())
			StorageNode.LOGGER.info("Old useless backup deleted: " + removedBackupForBucket);
	}

	private JSONArray retrieveJSONListFiles(){
		JSONArray output = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile.toJSONObject()));
		if(output.length() == 0)
			output.put(new JSONObject().put("status", "empty list"));
		return output;
	}

	public static int resolveConflictResolution(Long tsChoosen, String fileName){
		int ret = 0;
		byte[] rcvdFile = null;
		File filesDir = null;
		try {
			if(myFiles.containsKey(fileName)){
				VectorClock oldClock = myFiles.get(fileName).getVersion();
				ArrayList<Data> conflicts = myFiles.get(fileName).getConflicts();
				for(Data conflictData : conflicts){
					if(conflictData.getVersion().getTimestamp() == tsChoosen){
						conflictData.setIdFile(StorageNode.myFiles.get(fileName).getIdFile());
						conflictData.clearConflict();
						VectorClock newUpdatedClock = oldClock.merge(conflictData.getVersion());
						conflictData.setVersion(newUpdatedClock);
						StorageNode.myFiles.remove(fileName);
						StorageNode.myFiles.put(fileName, conflictData);
						int idrequestForConflict = StorageNode.requestIDCounter.getAndIncrement();
						ArrayList<ByteBuffer> virtualBucketFor = StorageNode.retrieveVirtualBucketForMember(fileName);
						String ipCurrentReplica = StorageNode.getIPFromID(StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId, virtualBucketFor.get(0)));
						StorageMessage msgForConflictResolution = new StorageMessage(StorageNode.myHost,
								ipCurrentReplica,
								Message.Type.REQUEST,
								idrequestForConflict,
								Message.Command.UPDATE_BACKUP,
								-1,
								null,
								fileName,
								conflictData,
								null);
						ret = idrequestForConflict;
						StorageNode.responseHandlerThread.addSingleIdToQueue(ret);
						StorageNode.pendingRequest.put(idrequestForConflict, msgForConflictResolution);
						StorageNode.senderThread.addSendRequestToQueue(msgForConflictResolution, ipCurrentReplica);
						rcvdFile = Base64.decodeBase64(conflictData.getFile());
						// save the file to disk
						filesDir = new File("./backup/");
						if(!filesDir.exists()){
							filesDir.mkdir();
						}
						File fileToSave = new File("./backup/" + fileName);
						Files.write(rcvdFile, fileToSave);
					}
				}				
			} else {
				StorageNode.LOGGER.warn("The given request for CONFLICT_RESOLUTION is wrong: no " + fileName + " file saved locally.");
				ret = -1;
			}
		} catch (IOException e) {
			StorageNode.LOGGER.error("An error occurred when saving file in local directory. " + fileName + ". Error = " + e.getStackTrace());
			ret = -1;
		}
		return ret;
	}

	// Method for processing Message request ONLY from other nodes
	private void processRequest(StorageMessage msg) throws Exception{
		int type = msg.getType();
		int command = msg.getCommand();
		switch(command){
		case Message.Command.GET:
			if(type == Message.Type.REQUEST){
				// check if data is in my bucket
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((myFiles.containsKey(msg.getFileName())) ? "status" : "error", 
						(myFiles.containsKey(msg.getFileName())) ? "ok" : "The requested file does not exists. " + msg.toJSONObject().toString()));
				Data dataForGet = myFiles.get(msg.getFileName());
				// Check if there aren't conflicts

				StorageMessage response = null;
				if(!output.getJSONObject(0).has("error")){
					if(dataForGet.hasConflict())
						response = new StorageMessage(StorageNode.myHost,
								msg.getHost(),
								Message.Type.RESPONSE, 
								msg.getIdRequest(), 
								msg.getCommand(), 
								Message.ReturnCode.CONFLICTS_EXISTS, 
								output, 
								msg.getFileName(), 
								(output.getJSONObject(0).has("error")) ? null : dataForGet, 
										null);
					else
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
			}
			break;
		case Message.Command.PUT:
			if(type == Message.Type.REQUEST){
				int ret = StorageNode.putData(msg.getData());
				JSONArray output = new JSONArray();
				if(ret == -1)
					output.put(new JSONObject().put("error", ""));
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					// Check if the return code is OK: if not, print an error
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				int ret = StorageNode.deleteData(msg.getFileName());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while deleting data: the given file does not exists. " + msg.toJSONObject().toString()));
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				ret = this.putBackupData(StorageNode.getIDFromIP(msg.getHost()), backup);
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				if(StorageNode.backupFiles.containsKey(StorageNode.getIDFromIP(msg.getHost()))){
					HashMap<String, Data> dataToDelete = StorageNode.backupFiles.remove(StorageNode.getIDFromIP(msg.getHost()));
					for(Data data : dataToDelete.values()) {
						File deleteFile = new File("./backup/" + data.getFileName());
						if(!deleteFile.delete()){
							dataToDelete.remove(data.getFileName());
							StorageNode.backupFiles.put(StorageNode.getIDFromIP(msg.getHost()), dataToDelete);
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
				StorageNode.executor.execute(senderThread);
			} else if(type == Message.Type.RESPONSE) {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				int ret = this.putBackupData(StorageNode.getIDFromIP(msg.getHost()), backup);
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				StorageNode.deleteBackupDatas(StorageNode.getIDFromIP(msg.getHost()));
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
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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
				int idRequestForUpdateBackup = StorageNode.resolveConflictResolution(msg.getOutput().getLong(0), msg.getFileName());
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
					StorageNode.executor.execute(senderThread);
					StorageNode.executor.execute(responseHandlerThread);
				}
			} else if(type == Message.Type.RESPONSE){
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
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

	@Override
	public void onReceivedMessage(StorageMessage receivedMsg) {
		try {
			processRequest(receivedMsg);
		} catch (Exception e) {
			StorageNode.LOGGER.warn("The system isn't able to process a received message. It will be dropped: " + receivedMsg.toJSONObject().toString() + " . Error = " + e.getStackTrace());
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
							System.out.println("PUT_BACKUP " + fileToSendForBackup + " in " + StorageNode.getIPFromID(newBucketReplica));
							int idRequestForPutBackup = StorageNode.requestIDCounter.getAndIncrement();
							StorageMessage messageForNewPutBackup = new StorageMessage(StorageNode.myHost,
									StorageNode.getIPFromID(newBucketReplica),
									Message.Type.REQUEST,
									idRequestForPutBackup,
									Message.Command.PUT_BACKUP,
									-1,
									null,
									null,
									null,
									backup);
							StorageNode.pendingRequest.put(idRequestForPutBackup, messageForNewPutBackup);
							StorageNode.senderThread.addSendRequestToQueue(messageForNewPutBackup, StorageNode.getIPFromID(newBucketReplica));
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
						ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(data.getFileName());
						if(!virtualBucketFor.isEmpty()){
							int idRequestUpdate;
							try {
								idRequestUpdate = this.putData(data);
								ids.add(idRequestUpdate);
							} catch (Exception e) {
								StorageNode.LOGGER.error("The system encountered an error while saving the new file " + filename + ". Please take care of this situation. Error = " + e.getStackTrace());
							}
						} else {
							StorageNode.LOGGER.warn("The file " + filename + " selected to be part of my files as replica, is not part of my real files. Please check this behaviour: maybe some useless backup files are still in there.");
						}
					});
					StorageNode.deleteBackupDatas(member.getId());
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
					ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(filename);
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
					ArrayList<ByteBuffer> virtualBucketFor = retrieveVirtualBucketForMember(data.getFileName());
					String ipCurrentReplica = StorageNode.getIPFromID(cHasher.getDescendantBucketKey(myId, virtualBucketFor.get(0)));
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
							StorageNode.getIPFromID(member.getId()),
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
			StorageNode.LOGGER.error("An error occurred while deteting bucket: " + member.getHost() + ":" + member.getId() + ". Error = " + e.getStackTrace());
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
