package it.cnr.isti.pad.fs.storage;

import java.io.File;
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

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;
import com.google.code.gossip.manager.GossipManager;

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

	// Runnable for sending messages to other nodes
	private StorageSenderThreadImpl senderThread = null;

	// Runnable-daemon for receiving messages from other nodes
	private StorageReceiverThreadImpl receiverThread = null;

	// Executor service used to run threads
	private ExecutorService executor = null;
	
	// IP Address of replica node
	private String ipReplica = null;
	
	// IP Address of precedent node
	private String ipPrecNode = null;

	public static final Logger LOGGER = Logger.getLogger(StorageNode.class);

	private static AtomicBoolean keepRunning;

	public StorageNode(File fileSettings) throws UnknownHostException, SocketException{
		myFiles = new ConcurrentHashMap<String, Data>();
		backupFiles = new ConcurrentHashMap<String, Data>();
		pendingRequest = new ConcurrentHashMap<Integer, StorageMessage>();
		grs = new GossipResourceService(fileSettings, this);
		executor = Executors.newCachedThreadPool();
		senderThread = new StorageSenderThreadImpl();
		receiverThread = new StorageReceiverThreadImpl();
		keepRunning = new AtomicBoolean(true);
		cHasher = new ConsistentHasherImpl<>(
				this.logBase2(grs.getGossipService().get_gossipManager().getMemberList().size(), 2),
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.SHA1);
		// TODO: the way waiting for MemberList has to be changed.
		while(grs.getGossipService().get_gossipManager().getMemberList().isEmpty());
		cHasher.addBucket(grs.getGossipService().get_gossipManager().getMyself().getId());
		grs.getGossipService().get_gossipManager().getMemberList().forEach(member -> cHasher.addBucket(member.getId()));
		String idReplica = cHasher.getDescendantBucketKey(grs.getGossipService().get_gossipManager().getMyself().getId());
		String idPrec = cHasher.getLowerKey(this.grs.getGossipService().get_gossipManager().getMyself().getId());
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId() == idReplica)
				this.ipReplica = member.getHost();
			else if(member.getId() == idPrec)
				this.ipPrecNode = member.getHost();
		}
		StorageNode.LOGGER.info("My replica is: IP="  + ipReplica + ";  ID="+ idReplica);
	}

	@Override
	public void run() {   
		File f = new File("./files/");
		char[] workchars = {'|', '/', '-', '\\'};
		ArrayList<String> names = new ArrayList<String>(Arrays.asList(f.list()));
		names.forEach(file -> 
		{
			cHasher.addMember(file);
			Data dataFile = new Data(StorageNode.localMembersIDCounter.getAndIncrement(), false, "root", file, "./files/", new VectorClock());
			StorageNode.myFiles.put(file, dataFile);
		});
		int idBackupRequest = StorageNode.requestIDCounter.get();
		StorageMessage askForBackup = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
				Message.Type.REQUEST, 
				StorageNode.requestIDCounter.getAndIncrement(), 
				Message.Command.ASK_FOR_BACKUP, 
				-1, 
				null, 
				null, 
				null, 
				null);
		ArrayList<Integer> idsListRequest = new ArrayList<Integer>();
		idsListRequest.add(StorageNode.requestIDCounter.get());
		StorageMessage askForFileList = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
				Message.Type.REQUEST,
				StorageNode.requestIDCounter.getAndIncrement(),
				Message.Command.LIST,
				-1,
				null,
				null,
				null,
				null);
		StorageNode.grs.getGossipService().get_gossipManager().getMemberList().forEach(node -> 
		{
			this.senderThread.addSendRequestToQueue(askForFileList, node.getHost());
			idsListRequest.add(StorageNode.requestIDCounter.get());
			askForFileList.setIdRequest(StorageNode.requestIDCounter.getAndIncrement());
		});
			this.senderThread.addSendRequestToQueue(askForBackup, this.ipPrecNode);
			this.executor.execute(this.senderThread);
		int i = 0;
			while(StorageNode.pendingRequest.size() > 0){
				System.out.printf("\r%s%c", "Please wait for node initialization...", workchars[i]);
				try {
					sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				i++;
				if(i == workchars.length-1)
					i = 0;
			}
		System.out.printf("\r%s\n", "Please wait for server initialization...done.");
		ArrayList<String> fileList = new ArrayList<String>();
		myFiles.forEach((name, file) -> fileList.add(name));
		// Reading incoming messages for list files
			idsListRequest.forEach(idRequest -> 
			{
				StorageMessage listResponse = StorageNode.pendingResponse.get(idRequest);
				if(listResponse.getReturnCode() != Message.ReturnCode.ERROR)
					fileList.add(listResponse.getData().getFileName());
				else
					StorageNode.LOGGER.warn("The list file command encountered a problem while receiving file's list from host = " + listResponse.getHost());
			});
		fileList.sort(new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareToIgnoreCase(s2);
			}
		});
		System.out.println("Welcome back!");
		System.out.println("This is your distributed file system command-line:");
		System.out.println("File list:");
		fileList.forEach(fileName -> System.out.println(fileName));
		System.out.println("type one of these commands: PUT:[filepath], GET:[filename], LIST, DELETE:[filename] or HELP to print additional explanation.");
		while (keepRunning.get()) {
			// TODO: manage input from user + send message and process requests
			Scanner reader = new Scanner(System.in);  // Reading from System.in
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
				// TODO: parse input from user and the pass clientMsg to processRequest
				// TODO: Print all files in the file system and then ask for a file name
				String fileName = reader.nextLine();
				ArrayList<String> bucketFor = this.retrieveBucketForMember(fileName);
				if(bucketFor.size() > 1)
					StorageNode.LOGGER.warn("More than one bucket for " + fileName);
				if(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getId().equals(bucketFor.get(0))){
					// The user ask for some data stored in this node.
					System.out.println(this.myFiles.get(fileName).toJSONObject());
				} else {
					String ipBucketForFile = "";
					for(LocalGossipMember member : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
						if(member.getId().equals(bucketFor))
							ipBucketForFile = member.getHost();
					}
					StorageMessage getRemoteFile = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
																	  Message.Type.REQUEST,
																	  StorageNode.requestIDCounter.getAndIncrement(),
																	  Message.Command.GET,
																	  -1,
																	  null,
																	  fileName,
																	  null,
																	  null);
					this.senderThread.addSendRequestToQueue(getRemoteFile, ipBucketForFile);
					this.executor.execute(this.senderThread);
				}
				break;
			case "PUT":
				System.out.println(input);
				break;
			case "LIST":
				System.out.println(input);
				break;
			case "DELETE":
				System.out.println(input);
				break;
			case "HELP":
				System.out.println(input);
				break;
			case "CONFLICT_RESOLUTION":
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

//	// TODO: return the filename or the ip of the responsible bucket
//	?????????????? a cosa serve ??????????????????
//	private JSONObject getDataOrBucket(Data file){
//		JSONObject ret = new JSONObject();
//		ArrayList<String> bucketForFile = StorageNode.retrieveBucketForMember(file.getFileName());
//		String bucket = "";
//		if(bucketForFile.size() == 1)
//			bucket = bucketForFile.get(0);
//		else
//			StorageNode.LOGGER.warn("More than one bucket for " + file.getFileName());
//		try {
//			if(!bucket.equals(grs.getGossipService().get_gossipManager().getMyself().getHost()))
//				ret.put("host", bucket);
//			else 
//				// Return filename of the given file, if present
//				ret.put("file", myFiles.containsKey(file.getFileName()));
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			StorageNode.LOGGER.warn("Error while processing GetData for: " + file.getFileName() + " \n " + e.getMessage());
//			return null;
//		}
//		return ret;
//	}

	// TODO: check version increment + implement as LOCAL_ONLY putdata:
	//		 the assumption is that this methods is called for update/put
	//		 only files that belongs to this bucket, otherwise return err.
	//		 P.S.: take care about replica copies
	//		 TODO for CMD Interface: whenever the same command is given by the 
	//						CMD user will call this method in order to check if
	//						the file has to be stored here or not.
	// return 0 in case of success; != 0 otherwise
	private int putData(StorageMessage msg){
		// Check if this file has to be stored in this Bucket or another
		ArrayList<String> bucketForFile = StorageNode.retrieveBucketForMember(msg.getFileName());
		String bucket = "";
		if(bucketForFile.size() > 1)
			StorageNode.LOGGER.warn("More than one bucket for " + msg.getFileName());
		bucket = bucketForFile.get(0);
		// Check if this file already exists
		Boolean trovato = false;
		// Check if the file is already 
		for(String member : cHasher.getAllMembers()){
			if(member.equals(msg.getFileName()))
				trovato = true;
		}
		if(trovato == false){
			StorageNode.myFiles.put(msg.getFileName(), msg. getData());
			StorageNode.localMembersIDCounter.getAndIncrement();
			StorageNode.cHasher.addMember(msg.getFileName());
			// Send file to replica node
			StorageMessage sendBackup = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
														   Message.Type.REQUEST, 
														   StorageNode.requestIDCounter.getAndIncrement(), 
														   Message.Command.UPDATE_BACKUP, 
														   -1, 
														   null, 
														   msg.getFileName(), 
														   StorageNode.myFiles.get(msg.getFileName()), 
														   null);
			// TODO: look at the behaviour of this queue: is one writer and one reader, but reader also deletes. Check if all is ok
			//		 and even for the thread: we don't know how executor will run this runnable so we cannot make any decisions, but take care of it.
			// FIX USED: pendingrequest is concurrent hash map
			this.senderThread.addSendRequestToQueue(sendBackup, this.ipReplica);
			executor.execute(this.senderThread);
		} else {
			// Update the given file here and in replica node
			StorageNode.myFiles.keySet().forEach(key ->
			{
				if(myFiles.get(key).getFileName().equals(msg.getFileName())){
					VectorClock myCopy = myFiles.get(key).getVersion();
					VectorClock newCopy = msg.getData().getVersion();
					switch (myCopy.compare(newCopy)) {
                    case BEFORE:
                    	// Do nothing
                       break;
                    case AFTER:
                    	// Update value and send update to replica node
                    	myFiles.put(key, msg.getData());
    					// Send updated value to backup copy
    					StorageMessage updateBackup = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
    																	 Message.Type.REQUEST,
    																	 StorageNode.requestIDCounter.getAndIncrement(),
    																	 Message.Command.UPDATE_BACKUP,
    																	 -1,
    																	 null,
    																	 msg.getFileName(),
    																	 StorageNode.myFiles.get(msg.getFileName()),
    																	 null);
    					this.senderThread.addSendRequestToQueue(updateBackup, this.ipReplica);
    					this.executor.execute(this.senderThread);
                    	break;
                    case CONCURRENTLY:
                    	// TODO: Manage conflict 
                        break;
					}
				}
			});
		}
		return 0;
	}


	/**
	 * 
	 * @param file
	 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
	 */
	private int putBackupData(Data file){
		Data oldCopy = this.backupFiles.getOrDefault(file.getFileName(), null);
		int ret = 0;
		if(oldCopy == null)
			StorageNode.backupFiles.put(file.getFileName(), file);
		else {
			VectorClock myBackupVector = oldCopy.getVersion();
			VectorClock fileVector = file.getVersion();
			switch (myBackupVector.compare(fileVector)) {
            case BEFORE:
            	// Do nothing
               break;
            case AFTER:
            	// Update value 
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

	// TODO: if the file is present locally, delete it with it's remote copy in replica node; otherwise return error.
	private int deleteData(Data file){
		return 0;
	}

	// TODO: retrieve list files from each nodes and then set the output with first OBJ with status/error and the other OBJ's files
	private JSONArray retrieveJSONListFiles(){
		JSONArray output = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile));
		return output;
	}

	public static ArrayList<Data> getListFiles(){
		return null;
	}

	public static boolean resolveConflict(Integer idFile){
		return true;
	}

	// TODO: refactor taking to account the fact that this request comes from other nodes.
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
				StorageMessage response = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
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
				// This is the case of a previous requests arrives to this node but this node is not responsible for the requested data.
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped.");
			}
			break;
		case Message.Command.PUT:
			if(type == Message.Type.REQUEST){
				int ret = this.putData(msg);
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while storing data. " + msg.toJSONObject().toString()));
				StorageMessage response = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
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
			} else {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped.");
			}
			break;
		case Message.Command.DELETE:
			if(type == Message.Type.REQUEST){
				int ret = this.deleteData(msg.getData());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while deleting data: the given file does not exists. " + msg.toJSONObject().toString()));
				StorageMessage response = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
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
			} else {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped.");
			}
			break;
		case Message.Command.LIST:
			if(type == Message.Type.REQUEST){
				JSONArray output = this.retrieveJSONListFiles();
				StorageMessage response = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
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
			} else {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped.");
			}
			break;

		case Message.Command.PUT_BACKUP:
			if(type == Message.Type.REQUEST){
				int ret = this.putBackupData(msg.getData());
				JSONArray output = new JSONArray();
				output.put(new JSONObject().put((ret > -1) ? "status" : "error", (ret > -1) ? "ok" : "Error while storing replica data. " + msg.toJSONObject().toString()));
				StorageMessage response = new StorageMessage(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost(), 
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
			} else {
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
				} else 
					// drop the packet
					StorageNode.LOGGER.warn("Receive a reponse message associated to any of pending id requests. The packet will be dropped.");
			}
			break;
		case Message.Command.DELETE_BACKUP:
			break;
		case Message.Command.UPDATE_BACKUP:
			break;
		case Message.Command.ASK_FOR_BACKUP:
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
			// TODO Auto-generated catch block
			e.printStackTrace();
			StorageNode.LOGGER.warn("The system isn't able to process a received message. It will be dropped: " + receivedMsg.toString());
		}
	}

	@Override
	public void gossipEvent(GossipMember member, GossipState state) {
		// If the state is down: send member's data to the new copy_replica and cleanup the env
		// If the state is up: put it in the env and rebalance all the CH's ring.
		System.out.println("bleeeeeeeeee listeneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
	}

}
