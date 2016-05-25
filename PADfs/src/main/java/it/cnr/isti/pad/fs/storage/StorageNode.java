package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;
import com.google.code.gossip.manager.GossipManager;
import com.google.common.io.Files;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import it.cnr.isti.pad.fs.udpsocket.Message;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageReceiverThreadImpl;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageSenderThreadImpl;
import voldemort.versioning.VectorClock;
import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.event.OnMessageReceivedListener;
import it.cnr.isti.pad.fs.gossiping.GossipResourceService;
import it.cnr.isti.pad.fs.storage.Data.Fields;

/** 
 * StorageNode class: it implements the logic of the storage functions of each node of the net.
 * 					  It also implement the GossipListener interface in order to listen for
 * 					  new UP/DOWN nodes.
 * */
public class StorageNode extends Thread implements GossipListener, OnMessageReceivedListener {

	private static AtomicInteger localMembersIDCounter = new AtomicInteger(0);

	private static AtomicInteger requestIDCounter = new AtomicInteger(0);

	// The files for which this node is responsible.
	private static ConcurrentHashMap<String, Data> myFiles;

	// The backup files for which this node acts as replica.
	private static ConcurrentHashMap<String, Data> backupFiles;

	// The Consistent Hasher instance
	private static ConsistentHasher<String, String> cHasher;

	// HashMap of my pending requests
	private static ConcurrentHashMap<Integer, StorageMessage> pendingRequest;

	// HashMap of my responses for pending requests
	private static ConcurrentHashMap<Integer, StorageMessage> pendingResponse;

	// Instance of Gossip Service
	public static GossipResourceService grs = null;
	
	// My personal ip
	private static String myHost = null;
	
	// My personal id
	private static String myId = null;

	// Runnable for sending messages to other nodes
	private StorageSenderThreadImpl senderThread = null;

	// Runnable-daemon for receiving messages from other nodes
	private StorageReceiverThreadImpl receiverThread = null;

	// Executor service used to run threads
	private ExecutorService executor = null;

	// IP Address of replica node
	private String ipReplica = null;
	
	// Id of the replica node
	private String idReplica = null;

	// IP Address of precedent node
	private String ipPrecNode = null;

	public static final Logger LOGGER = Logger.getLogger(StorageNode.class);

	private static AtomicBoolean keepRunning;

	public StorageNode(File fileSettings) throws UnknownHostException, SocketException{
		myFiles = new ConcurrentHashMap<String, Data>();
		backupFiles = new ConcurrentHashMap<String, Data>();
		pendingRequest = new ConcurrentHashMap<Integer, StorageMessage>();
		pendingResponse = new ConcurrentHashMap<Integer, StorageMessage>();
		grs = new GossipResourceService(fileSettings, this);
		executor = Executors.newCachedThreadPool();
		senderThread = new StorageSenderThreadImpl();
		receiverThread = new StorageReceiverThreadImpl();
		receiverThread.addListener(this);
		keepRunning = new AtomicBoolean(true);
		cHasher = new ConsistentHasherImpl<>(
				this.logBase2(grs.getGossipService().get_gossipManager().getMemberList().size(), 2),
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.SHA1);
		// TODO: the way waiting for MemberList has to be changed.
		while(grs.getGossipService().get_gossipManager().getMemberList().isEmpty());
		myHost = StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost();
		myId = grs.getGossipService().get_gossipManager().getMyself().getId();
		cHasher.addBucket(myId);
		grs.getGossipService().get_gossipManager().getMemberList().forEach(member -> cHasher.addBucket(member.getId()));
		this.idReplica = cHasher.getDescendantBucketKey(myId);
		String idPrec = cHasher.getLowerKey(myId);
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId() == idReplica)
				this.ipReplica = member.getHost();
			else if(member.getId() == idPrec)
				this.ipPrecNode = member.getHost();
		}
		StorageNode.LOGGER.info("My replica is: IP="  + ipReplica + ";  ID="+ idReplica);
	}
	
	private String getIPFromID(String id){
		String ret = "";
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId() == id)
				ret = member.getHost();
		}
		return ret;
	}

	@Override
	public void run() {
		Scanner reader = new Scanner(System.in);  // Reading from System.in
		File f = new File("./files/");
		char[] workchars = {'|', '/', '-', '\\'};
		ArrayList<String> names = new ArrayList<String>(Arrays.asList(f.list()));
		this.executor.execute(receiverThread);
		names.forEach(file -> 
		{
			cHasher.addMember(file);
			Data dataFile = new Data(StorageNode.localMembersIDCounter.getAndIncrement(), false, "root", file, "./files/", new VectorClock());
			StorageNode.myFiles.put(file, dataFile);
		});
		int idBackupRequest = StorageNode.requestIDCounter.getAndIncrement();
		StorageMessage askForBackup = new StorageMessage(StorageNode.myHost, 
				Message.Type.REQUEST, 
				idBackupRequest, 
				Message.Command.ASK_FOR_BACKUP, 
				-1, 
				null, 
				null, 
				null, 
				null);
		this.senderThread.addSendRequestToQueue(askForBackup, this.ipPrecNode);
		this.executor.execute(this.senderThread);
		int i = 0;
		System.out.println("Welcome back!");
		System.out.println("This is your distributed file system command-line:");
		System.out.println("type one of these commands: PUT:[filepath], GET:[filename], LIST, DELETE:[filename] or HELP to print additional explanation.");
		while (keepRunning.get()) {
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
			case "GET":
				String fileName = commandAndArgs.get(1);
				ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileName);
				if(bucketFor.size() > 1)
					StorageNode.LOGGER.warn("More than one bucket for " + fileName);
				if(StorageNode.myId.equals(bucketFor.get(0))){
					// The user ask for some data stored in this node.
					System.out.println("The request file is already present in your disk at: " + StorageNode.myFiles.get(fileName).getPathToFile() + StorageNode.myFiles.get(fileName).getFileName());
				} else {
					String ipBucketForFile = "";
					for(LocalGossipMember member : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
						if(member.getId().equals(bucketFor))
							ipBucketForFile = member.getHost();
					}
					int idRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage getRemoteFile = new StorageMessage(StorageNode.myHost, 
							Message.Type.REQUEST,
							idRequest,
							Message.Command.GET,
							-1,
							null,
							fileName,
							null,
							null);
					StorageNode.pendingRequest.put(idRequest, getRemoteFile);
					this.senderThread.addSendRequestToQueue(getRemoteFile, ipBucketForFile);
					this.executor.execute(this.senderThread);
					i = 0;
					while(StorageNode.pendingRequest.size() > 0){
						System.out.printf("\r%s%c", "Please wait while retrieving requested file...", workchars[i]);
						try {
							sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						i++;
						if(i == workchars.length-1)
							i = 0;
					}
					System.out.printf("\r%s\n", "Please wait while retrieving requested file...done.");
					StorageMessage responseForGetRequest = StorageNode.pendingResponse.get(idRequest);
					System.out.println("The requested file has been received and saved in: " + "./download/"+ responseForGetRequest.getFileName());
				}
				break;
			case "PUT":
				// TODO: parsing path from put command
				String fileNameForPut = commandAndArgs.get(1);
				ArrayList<String> bucketForPut = StorageNode.retrieveBucketForMember(fileNameForPut);
				StorageMessage msgForPut = null;
				if(bucketForPut.size() > 1)
					StorageNode.LOGGER.warn("More than one bucket for " + fileNameForPut);
				File newFile = new File("./"+fileNameForPut);
				Data newData = new Data(-1, false, "user1", fileNameForPut, "./", new VectorClock());
				if(StorageNode.myId.equals(bucketForPut.get(0))){
					int idUpdateBackupRequest = this.putData(newData);
					// if idUpdateBackupRequest != -1 => you can waiting for response and see if all is ok.
					System.out.println("The given file has been successfully saved locally in: ./files/"+ fileNameForPut);
				} else {
					// Forward request to right bucket
					String ipRightBucket = this.getIPFromID(bucketForPut.get(0));
					int currentIdRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage putRemoteFile = new StorageMessage(StorageNode.myHost,
																	  Message.Type.REQUEST,
																	  currentIdRequest,
																	  Message.Command.PUT,
																	  -1,
																	  null,
																	  fileNameForPut,
																	  newData,
																	  null);
					StorageNode.pendingRequest.put(currentIdRequest, putRemoteFile);
					this.senderThread.addSendRequestToQueue(putRemoteFile, ipRightBucket);
					this.executor.execute(this.senderThread);
					i = 0;
					while(StorageNode.pendingRequest.containsKey(currentIdRequest)){
						System.out.printf("\r%s%c", "The given file is stored on another node. Please wait while forwarding request to the right node...", workchars[i]);
						try {
							sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						i++;
						if(i == workchars.length-1)
							i = 0;
					}
					System.out.printf("\r%s\n", "The given file is stored on another node. Please wait while forwarding request to the right node...done.");
					StorageMessage responseForPut = StorageNode.pendingResponse.get(currentIdRequest);
					if(responseForPut.getReturnCode() == Message.ReturnCode.OK)
						System.out.println("The requested file has been saved in the remote node (" + ipRightBucket + ") successfully.");
					else
						System.out.println("A problem arise in the remote node performung put/update. Please try again.");
				}
				break;
			case "LIST":
				ArrayList<Integer> idsListRequest = new ArrayList<Integer>();
				for(LocalGossipMember node : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
					int currListIdRequest = StorageNode.requestIDCounter.getAndIncrement();
					idsListRequest.add(currListIdRequest);
					StorageMessage askForFileList = new StorageMessage(StorageNode.myHost, 
							Message.Type.REQUEST,
							currListIdRequest,
							Message.Command.LIST,
							-1,
							null,
							null,
							null,
							null);
					StorageNode.pendingRequest.put(currListIdRequest, askForFileList);
					this.senderThread.addSendRequestToQueue(askForFileList, node.getHost());
				}
				this.executor.execute(this.senderThread);
				i = 0;
				while(StorageNode.pendingRequest.size() > 0){
					System.out.printf("\r%s%c", "Please wait while retrieving file list from other nodes...", workchars[i]);
					try {
						sleep(200);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					i++;
					if(i == workchars.length-1)
						i = 0;
				}
				System.out.printf("\r%s\n", "Please wait while retrieving file list from other nodes...done.");
				ArrayList<String> fileList = new ArrayList<String>();
				myFiles.forEach((name, file) -> fileList.add(name));
				// Reading incoming messages for list files
				idsListRequest.forEach(idRequest -> 
				{
					StorageMessage listResponse = StorageNode.pendingResponse.get(idRequest);
					if(listResponse.getReturnCode() != Message.ReturnCode.ERROR){
						JSONArray receivedListFiles = listResponse.getOutput();
						for(int j = 0; j < receivedListFiles.length(); j++){
							Data newDataFromRemoteNode = null;
							try {
								newDataFromRemoteNode = new Data(receivedListFiles.getJSONObject(j));
								fileList.add(newDataFromRemoteNode.getFileName());
							} catch (Exception e) {
								e.printStackTrace();
								StorageNode.LOGGER.error("Error while parsing LIST response. Please try again.");
							}
						}
					} else
						StorageNode.LOGGER.warn("The list file command encountered a problem while receiving file's list from host = " + listResponse.getHost());
				});
				fileList.sort(new Comparator<String>() {
					@Override
					public int compare(String s1, String s2) {
						return s1.compareToIgnoreCase(s2);
					}
				});
				System.out.println("File list:");
				fileList.forEach(name -> System.out.println(name));
				break;
			case "DELETE":
				String fileNameForDelete = commandAndArgs.get(1);
				int idRequestDeleteBackup = this.deleteData(fileNameForDelete);
				if(idRequestDeleteBackup != -1){
					System.out.println("The given file has been successfully canceled.");
				} else {
					StorageNode.LOGGER.info("The file requested for delete is not in this node. Retrieving remote host's infos.");
					ArrayList<String> bucketForThisData = StorageNode.retrieveBucketForMember(fileNameForDelete);
					if(bucketForThisData.size() > 1)
						StorageNode.LOGGER.warn("More than one bucket for " + fileNameForDelete);
					String remoteip = this.getIPFromID(bucketForThisData.get(0));
					int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
					StorageMessage remoteDelMessage = new StorageMessage(StorageNode.myHost,
																		 Message.Type.REQUEST,
																		 currDeleteIdRequest,
																		 Message.Command.DELETE,
																		 -1,
																		 null,
																		 fileNameForDelete,
																		 null,
																		 null);
					StorageNode.pendingRequest.put(currDeleteIdRequest, remoteDelMessage);
					this.senderThread.addSendRequestToQueue(remoteDelMessage, remoteip);
					this.executor.execute(this.senderThread);
					// Wait for response
					i = 0;
					while(StorageNode.pendingRequest.size() > 0){
						System.out.printf("\r%s%c", "Please wait while processing your delete request...", workchars[i]);
						try {
							sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						i++;
						if(i == workchars.length-1)
							i = 0;
					}
					System.out.printf("\r%s\n", "Please wait while processing your delete request...done.");
					StorageMessage remoteDeleteResponse = StorageNode.pendingResponse.get(idRequestDeleteBackup);
					if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.OK)
						System.out.print("The given file has been successfully canceled.");
					else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.ERROR)
						System.out.println("The remote node encountered a problem while executing cancelation of the given file. Please try again.");
					else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.NOT_EXISTS)
						System.out.print("Error: the given file does not exists. Please check filename and try again.");
				}
				 
				break;
			case "HELP":
				System.out.println(input);
				break;
			case "CONFLICT_RESOLUTION":
				break;
			case "QUIT":
				this.receiverThread.shutdown();
				StorageNode.keepRunning.set(false);
				break;
			default:
				System.out.println(input + ": Command not recognized.");
			}
		}
	}

	public void shutdown(){
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

	// PutData: the assumption is that this methods is called for update/put
	//		 only files that belongs to this bucket, otherwise return err.
	//		 P.S.: take care about replica copies
	// RETURNS: the id of the request for UPDATE_BACKUP (if the file is new), -1 otherwise
	private int putData(Data dataFile){
//		ArrayList<String> bucketForFile = StorageNode.retrieveBucketForMember(dataFile.getFileName());
//		String bucket = "";
//		if(bucketForFile.size() > 1)
//			StorageNode.LOGGER.warn("More than one bucket for " + dataFile.getFileName());
//		bucket = bucketForFile.get(0);
		int ret = -1;
		// Check if this file already exists
		Boolean trovato = false;
		// Check if the file is already 
		for(String member : cHasher.getAllMembers()){
			if(member.equals(dataFile.getFileName()))
				trovato = true;
		}
		if(trovato == false){
			int idNewFile = StorageNode.localMembersIDCounter.getAndIncrement();
			dataFile.setIdFile(idNewFile);
			StorageNode.myFiles.put(dataFile.getFileName(), dataFile);
			StorageNode.cHasher.addMember(dataFile.getFileName());
			int currentIDREQ = StorageNode.requestIDCounter.getAndIncrement();
			// Send file to replica node
			StorageMessage sendBackup = new StorageMessage(StorageNode.myHost, 
					Message.Type.REQUEST, 
					currentIDREQ, 
					Message.Command.UPDATE_BACKUP, 
					-1, 
					null, 
					dataFile.getFileName(), 
					StorageNode.myFiles.get(dataFile.getFileName()), 
					null);
			// TODO: look at the behaviour of this queue: is one writer and one reader, but reader also deletes. Check if all is ok
			//		 and even for the thread: we don't know how executor will run this runnable so we cannot make any decisions, but take care of it.
			// FIX USED: pendingrequest is concurrent hash map
			StorageNode.pendingRequest.put(currentIDREQ, sendBackup);
			this.senderThread.addSendRequestToQueue(sendBackup, this.ipReplica);
			executor.execute(this.senderThread);
			ret = currentIDREQ;
		} else {
			// Update the given file here and in replica node
			for(String key : StorageNode.myFiles.keySet()){
				if(myFiles.get(key).getFileName().equals(dataFile.getFileName())){
					VectorClock myCopy = myFiles.get(key).getVersion();
					VectorClock newCopy = dataFile.getVersion();
					switch (myCopy.compare(newCopy)) {
					case BEFORE:
						// Do nothing
						break;
					case AFTER:
						// Update value and send update to replica node
						dataFile.setIdFile(myFiles.get(key).getIdFile());
						myFiles.remove(key);
						myFiles.put(key, dataFile);
						int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
						// Send updated value to backup copy
						StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
								Message.Type.REQUEST,
								idRequestForUpdateBackup,
								Message.Command.UPDATE_BACKUP,
								-1,
								null,
								dataFile.getFileName(),
								StorageNode.myFiles.get(dataFile.getFileName()),
								null);
						ret = idRequestForUpdateBackup;
						StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
						this.senderThread.addSendRequestToQueue(updateBackup, this.ipReplica);
						this.executor.execute(this.senderThread);
						break;
					case CONCURRENTLY:
						// TODO: Manage conflict 
						break;
					}
				}
			}
		}
		return ret;
	}


	/**
	 * 
	 * @param file
	 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
	 */
	private int putBackupData(Data file){
		Data oldCopy = StorageNode.backupFiles.getOrDefault(file.getFileName(), null);
		int ret = 0;
		if(oldCopy == null){
			file.setReplica(true);
			StorageNode.backupFiles.put(file.getFileName(), file);
			// TODO: store new files in ./backup/
		} else {
			VectorClock myBackupVector = oldCopy.getVersion();
			VectorClock fileVector = file.getVersion();
			switch (myBackupVector.compare(fileVector)) {
			case BEFORE:
				// Do nothing
				break;
			case AFTER:
				// Update value 
				file.setIdFile(StorageNode.backupFiles.get(file.getFileName()).getIdFile());
				StorageNode.backupFiles.replace(file.getFileName(), file);
				ret = 1;
				break;
			case CONCURRENTLY:
				// TODO: Manage conflict. Maybe useless for backup data?
				ret = -1;
				break;
			}
		}
		return ret;
	}

	private int deleteData(String fileName){
		int ret = 0;
		// Check if this file already exists
		Boolean trovato = false;
		// Check if the file is already 
		for(String member : cHasher.getAllMembers()){
			if(member.equals(fileName))
				trovato = true;
		}
		if(trovato == false){
			// The requested file does not exists
			StorageNode.LOGGER.warn("The file " + fileName + " does not exists and so cannot be deleted.");
			ret = -1;
		} else {
			cHasher.removeMember(fileName);
			myFiles.remove(fileName);
			int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage requestForDeleteBackup = new StorageMessage(StorageNode.myHost, 
																	   Message.Type.REQUEST,
																	   currDeleteIdRequest,
																	   Message.Command.DELETE_BACKUP,
																	   -1,
																	   null,
																	   fileName,
																	   null,
																	   null);
			ret = currDeleteIdRequest;
			StorageNode.pendingRequest.put(currDeleteIdRequest, requestForDeleteBackup);
			this.senderThread.addSendRequestToQueue(requestForDeleteBackup, this.ipReplica);
			this.executor.execute(this.senderThread);
		}
		return ret;
	}

	private JSONArray retrieveJSONListFiles(){
		JSONArray output = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile.toJSONObject()));
		return output;
	}
	
	public static boolean resolveConflict(Integer idFile){
		return true;
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
				StorageMessage response = new StorageMessage(StorageNode.myHost, 
						Message.Type.RESPONSE, 
						msg.getIdRequest(), 
						msg.getCommand(), 
						(output.getJSONObject(0).has("error")) ? Message.ReturnCode.NOT_EXISTS : Message.ReturnCode.OK, 
								output, 
								msg.getFileName(), 
								(output.getJSONObject(0).has("error")) ? null : myFiles.get(msg.getFileName()), 
										null);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
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
				int ret = this.putData(msg.getData());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put("status", "ok"));
				StorageMessage response = new StorageMessage(StorageNode.myHost, 
						Message.Type.RESPONSE, 
						msg.getIdRequest(),
						msg.getCommand(),
						(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
								output, 
								msg.getFileName(), 
								null, 
								null);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
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
				int ret = this.deleteData(msg.getData().getFileName());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while deleting data: the given file does not exists. " + msg.toJSONObject().toString()));
				StorageMessage response = new StorageMessage(StorageNode.myHost, 
						Message.Type.RESPONSE, 
						msg.getIdRequest(), 
						msg.getCommand(),
						(output.getJSONObject(0).has("error")) ? Message.ReturnCode.NOT_EXISTS : Message.ReturnCode.OK, 
								output, 
								msg.getFileName(), 
								null,
								null);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
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
															Message.Type.RESPONSE, 
															msg.getIdRequest(), 
															msg.getCommand(),
															(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
															output, 
															null, 
															null,
															null);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
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
				JSONArray backupCopies = msg.getBackup();
				int ret = 0;
				for(int i = 0; i < backupCopies.length() && (ret != -1); i++){
					Data backupCopy = new Data(backupCopies.getJSONObject(i));
					backupCopy.setPathToFile("./backup/");
					ret = this.putBackupData(backupCopy);
				}
				StorageMessage putBackupResponse = new StorageMessage(StorageNode.myHost,
																	 Message.Type.RESPONSE,
																	 msg.getIdRequest(),
																	 msg.getCommand(),
																	 (ret != -1) ? Message.ReturnCode.OK : Message.ReturnCode.ERROR,
																	 null,
																	 null,
																	 null,
																	 null);
				this.senderThread.addSendRequestToQueue(putBackupResponse, msg.getHost());
				this.executor.execute(this.senderThread);
			} else if(type == Message.Type.RESPONSE){
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					if(msg.getReturnCode() != Message.ReturnCode.OK)
						StorageNode.LOGGER.warn("A problem arise while send backup copies to " + msg.getHost() + " host.");
					else
						StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully update/add copy of " + msg.getFileName() + ".");
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
			}
			break;
		case Message.Command.DELETE_BACKUP:
			if(type == Message.Type.REQUEST){
				StorageMessage responseForDeleteBackup = null;
				if(StorageNode.backupFiles.contains(msg.getFileName())){
					StorageNode.backupFiles.remove(msg.getFileName());
					responseForDeleteBackup = new StorageMessage(StorageNode.myHost, 
																				Message.Type.RESPONSE, 
																				msg.getIdRequest(),
																				Message.Command.DELETE_BACKUP,
																				Message.ReturnCode.OK, 
																				null, 
																				msg.getFileName(), 
																				null, 
																				null);
				} else 
					responseForDeleteBackup = new StorageMessage(StorageNode.myHost, 
																 Message.Type.RESPONSE, 
																 msg.getIdRequest(),
																 Message.Command.DELETE_BACKUP,
																 Message.ReturnCode.NOT_EXISTS, 
																 null, 
																 msg.getFileName(), 
																 null, 
																 null);
				this.senderThread.addSendRequestToQueue(responseForDeleteBackup, msg.getHost());
				this.executor.execute(senderThread);
			} else if(type == Message.Type.RESPONSE) {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
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
				int ret = this.putBackupData(msg.getData());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while storing replica data. " + msg.toJSONObject().toString()));
				StorageMessage response = new StorageMessage(StorageNode.myHost, 
															Message.Type.RESPONSE, 
															msg.getIdRequest(), 
															msg.getCommand(),
															(output.getJSONObject(0).has("error")) ? Message.ReturnCode.ERROR : Message.ReturnCode.OK, 
															output, 
															msg.getFileName(), 
															null,
															null);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
			} else if(type == Message.Type.RESPONSE) {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					if(msg.getReturnCode() != Message.ReturnCode.OK)
						StorageNode.LOGGER.warn("A problem arise while sending backup copy to " + msg.getHost() + " host.");
					else
						StorageNode.LOGGER.info("The " + msg.getHost() + " host has successfully received backup copy.");
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped. " + msg.toJSONObject());
			}
			break;
		case Message.Command.ASK_FOR_BACKUP:
			if(type == Message.Type.REQUEST){
				JSONArray backup = new JSONArray();
				StorageNode.myFiles.forEach((fileName, dataFile) -> backup.put(dataFile.toJSONObjectWithFile()));
				StorageMessage response = new StorageMessage(StorageNode.myHost,
															Message.Type.RESPONSE,
															msg.getIdRequest(),
															Message.Command.PUT_BACKUP,
															Message.ReturnCode.OK,
															null,
															null,
															null,
															backup);
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
			} else if(type == Message.Type.RESPONSE) {
				// We use PUT_BACKUP as a Response of this type of message
			}
			break;
		case Message.Command.ERASE_BACKUP:
			if(type == Message.Type.REQUEST){
				String newIdReplica = cHasher.getLowerKey(StorageNode.myId);
				int idBackupRequest = StorageNode.requestIDCounter.getAndIncrement();
				StorageMessage eraseResponse = null;
				if(!newIdReplica.equals(this.idReplica)){
					this.ipReplica = this.getIPFromID(newIdReplica);
					this.idReplica = newIdReplica;
					StorageNode.backupFiles.clear();
					StorageMessage askForBackup = new StorageMessage(StorageNode.myHost, 
																	Message.Type.REQUEST, 
																	idBackupRequest, 
																	Message.Command.ASK_FOR_BACKUP, 
																	-1, 
																	null, 
																	null, 
																	null, 
																	null);
					this.senderThread.addSendRequestToQueue(askForBackup, this.ipReplica);
					eraseResponse = new StorageMessage(StorageNode.myHost,
																	 Message.Type.RESPONSE,
																	 msg.getIdRequest(),
																	 msg.getCommand(),
																	 Message.ReturnCode.OK,
																	 null,
																	 null,
																	 null,
																	 null);
				} else {
					eraseResponse = new StorageMessage(StorageNode.myHost,
							 Message.Type.RESPONSE,
							 msg.getIdRequest(),
							 msg.getCommand(),
							 Message.ReturnCode.ERROR,
							 null,
							 null,
							 null,
							 null);
				}
				this.senderThread.addSendRequestToQueue(eraseResponse, msg.getHost());
				this.executor.execute(this.senderThread);		
			} else if(type == Message.Type.RESPONSE){
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
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
			break;
		}
	}

	@Override
	public void onReceivedMessage(StorageMessage receivedMsg) {
		try {
			processRequest(receivedMsg);
		} catch (Exception e) {
			e.printStackTrace();
			StorageNode.LOGGER.warn("The system isn't able to process a received message. It will be dropped: " + receivedMsg.toJSONObject().toString() + " : " + e.getMessage());
		}
	}

	@Override
	public void gossipEvent(GossipMember member, GossipState state) {
		// TODO: if member == ipReplica =>
		// 						If the state is down: send member's data to the new copy_replica and cleanup the env
		// 						If the state is up: check if it is your next.
		// TODO: implement timeout messages => when a node die from gossip, clean all pending requests
		// TODO: if member == ipForWhichIActAsReplica =>
		//						If state is down: you has to pass all your backup data in myfiles and DO NOT change isReplica=true: this information could be useful
		//						if stat is up: you send to it all updated data; you have to check if you are still the replica node: if yes => ok; if no delete all file and ask_for_backup
		// TODO: if member == ? => // valid only in a dynamic node insertion scenario
		//						check if new node: in this case you have to check if you have to send it some data (if it is your precedent)
	}

}
