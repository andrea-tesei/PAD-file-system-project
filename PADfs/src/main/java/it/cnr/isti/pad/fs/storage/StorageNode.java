package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.Instant;
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
import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
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
// TODO: start thread for handling response when UPDATE_BACKUP and PUT => ok (to be tested) + check how delete pending request handling => ok (to be tested) + confict resolution => ok (to be tested) + Adjust vectoclock use => TBD (to be tested) + refactoring + api
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
	private StorageSenderThreadImpl senderThread = null;

	// Runnable-daemon for receiving messages from other nodes
	private StorageReceiverThreadImpl receiverThread = null;

	private StorageResponseAsyncHandler responseHandlerThread = null;

	// Executor service used to run threads
	private ExecutorService executor = null;

	// IP Address of replica node
	private String ipReplica = null;

	// Id of the replica node
	private String idReplica = null;

	// IP Address of precedent node
	private String ipPrecNode = null;

	// ID of precedent node
	private String idPrecNode = null;


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
				e.printStackTrace();
			}
			i++;
			if(i == workchars.length-1)
				i = 0;
		}
		System.out.printf("\r%s\n", "Please wait for server initialization...done.");
		while(grs.getGossipService().get_gossipManager().getMemberList().isEmpty());
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
		this.idReplica = cHasher.getDescendantBucketKey(myId);
		this.idPrecNode = cHasher.getLowerKey(myId);
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId() == idReplica)
				this.ipReplica = member.getHost();
			else if(member.getId() == this.idPrecNode)
				this.ipPrecNode = member.getHost();
		}
		StorageNode.LOGGER.info("My replica is: IP="  + ipReplica + ";  ID="+ idReplica);
		StorageNode.LOGGER.info("My precedent node is: IP=" + ipPrecNode + "; ID=" + idPrecNode);
	}

	private String getIPFromID(String id){
		String ret = "";
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getId().equals(id))
				ret = member.getHost();
		}
		return ret;
	}

	private String getIDFromIP(String ip){
		String ret = "";
		for(LocalGossipMember member: grs.getGossipService().get_gossipManager().getMemberList()){
			if(member.getHost().equals(ip))
				ret = member.getId();
		}
		return ret;
	}

	@Override
	public void run() {
		Scanner reader = new Scanner(System.in);  // Reading from System.in
		//		File f = new File("./files/");
		File fileSavedBefore = new File("./files/storagedata.json");
		//		File[] files = f.listFiles();
		this.executor.execute(receiverThread);
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
						e.printStackTrace();
					}
				});
			} catch (IOException e3) {
				e3.printStackTrace();
				StorageNode.LOGGER.error("An error occurs while reading files from local directory. Path=./files/storagedata.json");
			}
		}
		JSONArray backup = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> backup.put(dataFile.toJSONObjectWithFile()));
		// Synchronization for debug
		//		try {
		//			sleep(2000);
		//		} catch (InterruptedException e2) {
		//			e2.printStackTrace();
		//		}
		int i = 0;
		int attempts = 0;
		if(!StorageNode.myFiles.isEmpty()){
			int idBackupRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage putBackup = new StorageMessage(StorageNode.myHost,
					this.ipPrecNode,
					Message.Type.REQUEST, 
					idBackupRequest, 
					Message.Command.PUT_BACKUP, 
					-1, 
					null, 
					null, 
					null, 
					backup);
			StorageNode.pendingRequest.put(idBackupRequest, putBackup);
			this.senderThread.addSendRequestToQueue(putBackup, this.ipPrecNode);
			this.executor.execute(this.senderThread);
			i = 0;
			attempts = 0;
			while(StorageNode.pendingRequest.size() > 0 && attempts < 20){
				System.out.printf("\r%s%c", "Please wait: the backup node is receiving files...", workchars[i]);
				try {
					sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				i++;
				attempts++;
				if(i == workchars.length-1)
					i = 0;
			}
			if(StorageNode.pendingResponse.get(idBackupRequest) != null){
				if(StorageNode.pendingResponse.get(idBackupRequest).getReturnCode() == Message.ReturnCode.ACCEPTED_TO_CHECK){
					StorageNode.LOGGER.warn("The node slave said that it has different view of the network: it accepts my backup but it has to check if i'm right.");
				}
			} else {
				StorageNode.LOGGER.fatal("We don't receive anything from the remote slave node. Check your internet connection.");
				//			return;
			}
			System.out.printf("\r%s\n", "Please wait: the backup node is receiving files...done.");
		} 
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
				if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
					String fileName = commandAndArgs.get(1);
					ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileName);
					if(bucketFor.size() > 1){
						StorageNode.LOGGER.warn("More than one bucket for " + fileName);
					} else if(!bucketFor.isEmpty()){
						if(StorageNode.myId.equals(bucketFor.get(0))){
							// The user ask for some data stored in this node.
							if(StorageNode.myFiles.get(fileName).hasConflict()){
								System.out.println("The requested file has some conflicts which must be resolved. Please choose one of these version and type the selected timestamp:");
								Data dataToSolve = StorageNode.myFiles.get(fileName);
								dataToSolve.getConflicts().forEach(conflictdata -> System.out.println(conflictdata.getVersion().getTimestamp() + ":" + conflictdata.getFileName()));
								String tsChoosen = reader.nextLine();
								for(Data conflictData :dataToSolve.getConflicts()){
									if(conflictData.getVersion().getTimestamp() == Long.parseLong(tsChoosen)){
										// Save the choosen version locally
										VectorClock oldClock = myFiles.get(fileName).getVersion();
										VectorClock updatedClock = conflictData.getVersion().merge(oldClock);
										conflictData.setVersion(updatedClock);
										myFiles.remove(fileName);
										myFiles.put(fileName, conflictData);
										byte[] rcvdFile = Base64.decodeBase64(conflictData.getFile());
										// save the file to disk
										File filesDir = new File("./download");
										try {
											if(!filesDir.exists()){
												filesDir.mkdir();
											}
											File fileToSave = new File("./download/" + conflictData.getFileName());
											Files.write(rcvdFile, fileToSave);
											System.out.println("The requested file has been received and saved in: " + "./download/"+ conflictData.getFileName());
										} catch (IOException e) {
											e.printStackTrace();
											System.err.println("An error occurs while saving file in " + "./download/" + conflictData.getFileName());
										}
									}

								}
							}
							System.out.println("The request file is already present in your disk at: " + StorageNode.myFiles.get(fileName).getPathToFile() + StorageNode.myFiles.get(fileName).getFileName());
						} else {
							String ipBucketForFile = "";
							for(LocalGossipMember member : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
								if(member.getId().equals(bucketFor.get(0)))
									ipBucketForFile = member.getHost();
							}
							int idRequest = StorageNode.requestIDCounter.getAndIncrement();
							StorageMessage getRemoteFile = new StorageMessage(StorageNode.myHost,
									ipBucketForFile,
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
							attempts = 0; 
							while(StorageNode.pendingRequest.containsKey(idRequest) && attempts < 20){
								System.out.printf("\r%s%c", "Please wait while retrieving requested file...", workchars[i]);
								try {
									sleep(250);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								i++;
								attempts++;
								if(i == workchars.length-1)
									i = 0;
							}
							System.out.printf("\r%s\n", "Please wait while retrieving requested file...done.");
							StorageMessage responseForGetRequest = StorageNode.pendingResponse.get(idRequest);
							if(responseForGetRequest != null){
								if(responseForGetRequest.getReturnCode() == Message.ReturnCode.OK){
									byte[] rcvdFile = Base64.decodeBase64(responseForGetRequest.getData().getFile());
									// save the file to disk
									File filesDir = new File("./download");
									try {
										if(!filesDir.exists()){
											filesDir.mkdir();
										}
										File fileToSave = new File("./download/" + responseForGetRequest.getData().getFileName());
										Files.write(rcvdFile, fileToSave);
										System.out.println("The requested file has been received and saved in: " + "./download/"+ responseForGetRequest.getFileName());
									} catch (IOException e) {
										e.printStackTrace();
										System.err.println("An error occurs while saving file in " + "./download/" + responseForGetRequest.getData().getFileName());
									}
								} else if(responseForGetRequest.getReturnCode() == Message.ReturnCode.CONFLICTS_EXISTS){
									System.out.println("The requested file has some conflicts which must be resolved. Please choose one of these version and type the selected timestamp:");
									// Print all conflict version: timestamp:filename
									responseForGetRequest.getOutput().forEach(conflictdata -> 
									{
										try {
											Data dataToBeChoose = new Data((JSONObject) conflictdata);
											System.out.println(dataToBeChoose.getVersion().getTimestamp() + ":" + dataToBeChoose.getFileName());
										} catch (Exception e) {
											e.printStackTrace();
											StorageNode.LOGGER.error("The system encountered an error during parsing received conflict data. " + conflictdata);
										}
									});
									// Wait timestamp and send CONFLICT_RESOLUTION message
									String tsChoosen = reader.nextLine();
									int idrequestForConflict = StorageNode.requestIDCounter.getAndIncrement();
									StorageMessage msgForConflictResolution = new StorageMessage(StorageNode.myHost,
											responseForGetRequest.getHost(),
											Message.Type.REQUEST,
											idrequestForConflict,
											Message.Command.CONFLICT_RESOLUTION,
											-1,
											null,
											fileName,
											null,
											null);
									responseForGetRequest.getOutput().forEach(conflictdata -> 
									{
										Data dataChoosen;
										try {
											dataChoosen = new Data((JSONObject) conflictdata);
											if(dataChoosen.getVersion().getTimestamp() == Long.parseLong(tsChoosen)){
												msgForConflictResolution.setData(dataChoosen);
												byte[] rcvdFile = Base64.decodeBase64(dataChoosen.getFile());
												// save the file to disk
												File filesDir = new File("./download");
												try {
													if(!filesDir.exists()){
														filesDir.mkdir();
													}
													File fileToSave = new File("./download/" + dataChoosen.getFileName());
													Files.write(rcvdFile, fileToSave);
													System.out.println("The requested file has been received and saved in: " + "./download/"+ dataChoosen.getFileName());
												} catch (IOException e) {
													e.printStackTrace();
													System.err.println("An error occurs while saving file in " + "./download/" + dataChoosen.getFileName());
												}
											}
										} catch (Exception e) {
											e.printStackTrace();
											StorageNode.LOGGER.error("The system encountered an error during parsing received conflict data. " + conflictdata);
										}
									});
									StorageNode.pendingRequest.put(idrequestForConflict, msgForConflictResolution);
									this.senderThread.addSendRequestToQueue(msgForConflictResolution, responseForGetRequest.getHost());
									this.executor.execute(this.senderThread);
									i = 0;
									attempts = 0; 
									while(StorageNode.pendingRequest.containsKey(idrequestForConflict) && attempts < 20){
										System.out.printf("\r%s%c", "Please wait while send message for conflict resolution...", workchars[i]);
										try {
											sleep(250);
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
										i++;
										attempts++;
										if(i == workchars.length-1)
											i = 0;
									}
									System.out.printf("\r%s\n", "Please wait while send message for conflict resolution...done.");
									StorageMessage responseForConflict = StorageNode.pendingResponse.get(idrequestForConflict);
									if(responseForConflict != null){
										if(responseForConflict.getReturnCode() == Message.ReturnCode.OK)
											StorageNode.LOGGER.info("The request for conflict resolution has been done successfully");
										else
											System.err.println("The request for conflict resolution encountered a problem in remote node. This will probably cause an inconsistency.");
									} else 
										System.err.println("No responces from remote node for CONFLICT_RESOLUTION of " + fileName);
								}
							} else
								System.err.println("The remote host did not receive the GET request. Please retry.");
						}
					} else {
						if(!StorageNode.myFiles.containsKey(fileName))
							System.err.print("The requested file is not present in the file system.");
						else {
							// Check if some conflict is present in the requested file
							if(!StorageNode.myFiles.get(fileName).hasConflict())
								System.out.println("The requested file is already present in your disk at: " + StorageNode.myFiles.get(fileName).getPathToFile() + StorageNode.myFiles.get(fileName).getFileName());
							else {
								System.out.println("The requested file has some conflicts which must be resolved. Please choose one of these version and type the selected timestamp:");
								StorageNode.myFiles.get(fileName).getConflicts().forEach(conflictdata ->
								// Print all conflict version: timestamp:filename
								System.out.println(conflictdata.getVersion().getTimestamp() + ":" + conflictdata.getFileName()));
								// Wait timestamp and send CONFLICT_RESOLUTION message
								String tsChoosen = reader.nextLine();
								Data choosenVersion = null;
								// TODO: to be checked. In principle we want to avoid that user feels that we are updating something outside my node. I do it asynchronously
								for(Data conflictdata : StorageNode.myFiles.get(fileName).getConflicts()) {
									if(conflictdata.getVersion().getTimestamp() == Long.parseLong(tsChoosen)){
										conflictdata.setIdFile(StorageNode.myFiles.get(fileName).getIdFile());
										conflictdata.clearConflict();
										VectorClock oldClock = StorageNode.myFiles.get(fileName).getVersion();
										VectorClock newUpdatedClock = oldClock.merge(conflictdata.getVersion());
										conflictdata.setVersion(newUpdatedClock);
										StorageNode.myFiles.remove(fileName);
										StorageNode.myFiles.put(fileName, conflictdata);
										int idrequestForConflict = StorageNode.requestIDCounter.getAndIncrement();
										StorageMessage msgForConflictResolution = new StorageMessage(StorageNode.myHost,
												this.ipReplica,
												Message.Type.REQUEST,
												idrequestForConflict,
												Message.Command.CONFLICT_RESOLUTION,
												-1,
												null,
												fileName,
												conflictdata,
												null);
										byte[] rcvdFile = Base64.decodeBase64(conflictdata.getFile());
										// save the file to disk
										File filesDir = new File("./download");
										try {
											if(!filesDir.exists()){
												filesDir.mkdir();
											}
											File fileToSave = new File("./download/" + conflictdata.getFileName());
											Files.write(rcvdFile, fileToSave);
											System.out.println("The requested file has been received and saved in: " + "./download/"+ conflictdata.getFileName());
										} catch (IOException e) {
											e.printStackTrace();
											System.err.println("An error occurs while saving file in " + "./download/" + conflictdata.getFileName());
										}
										this.senderThread.addSendRequestToQueue(msgForConflictResolution, this.ipReplica);
										StorageNode.pendingRequest.put(idrequestForConflict, msgForConflictResolution);
										this.responseHandlerThread.addSingleIdToQueue(idrequestForConflict);
										this.executor.execute(this.senderThread);
										this.executor.execute(this.responseHandlerThread);
									}
								}
							}
						}
					}
				} else 
					System.err.println("Error in GET: please specify the name of the file you want to download.");
				break;
			case "PUT":
				// TODO: parsing path from put command
				if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
					String fileNameForPut = commandAndArgs.get(1);
					ArrayList<String> bucketForPut = StorageNode.retrieveBucketForMember(fileNameForPut);
					StorageMessage msgForPut = null;
					byte[] fileBA;
					try {
						if(bucketForPut.size() > 1){
							StorageNode.LOGGER.warn("More than one bucket for " + fileNameForPut);
						} else if (!bucketForPut.isEmpty()){
							File newFile = new File("./"+fileNameForPut);
							fileBA = Files.toByteArray(newFile);
							VectorClock newVC = new VectorClock();
							newVC.incrementVersion(StorageNode.myId.hashCode(), System.currentTimeMillis());
							Data newData = new Data(-1, false, "root", bucketForPut.get(0), fileNameForPut, "./files/", Base64.encodeBase64String(fileBA), newVC);
							if(StorageNode.myId.equals(bucketForPut.get(0))){
								int idUpdateBackupRequest = this.putData(newData);
								System.out.println("The given file has been successfully saved in: ./files/"+ fileNameForPut);
								this.executor.execute(this.senderThread);
								i = 0;
								attempts = 0;
								while(StorageNode.pendingRequest.containsKey(idUpdateBackupRequest) && attempts < 20){
									System.out.printf("\r%s%c", "Please wait while update backup copy...", workchars[i]);
									try {
										sleep(250);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
									i++;
									attempts++;
									if(i == workchars.length-1)
										i = 0;
								}
								System.out.printf("\r%s\n", "Please wait while update backup copy...done.");
								StorageMessage responseForUpdateBackup = StorageNode.pendingResponse.get(idUpdateBackupRequest);
								if(responseForUpdateBackup != null){
									if(responseForUpdateBackup.getReturnCode() == Message.ReturnCode.OK)
										System.out.println("The remote slave node updated the new file successfully.");
									else
										System.err.println("The remote slave node encountered a problem while updating new file. However your local copy is ok.");
								} else
									System.err.println("The remote slave node did not receive UPDATE_BACKUP message. Please try again later.");
							} else {
								// Forward request to right bucket
								String ipRightBucket = this.getIPFromID(bucketForPut.get(0));
								int currentIdRequest = StorageNode.requestIDCounter.getAndIncrement();
								StorageMessage putRemoteFile = new StorageMessage(StorageNode.myHost,
										ipRightBucket,
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
								attempts = 0;
								while(StorageNode.pendingRequest.containsKey(currentIdRequest) && attempts < 20){
									System.out.printf("\r%s%c", "The given file is stored on another node. Please wait while forwarding request to the right node...", workchars[i]);
									try {
										sleep(250);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
									i++;
									attempts++;
									if(i == workchars.length-1)
										i = 0;
								}
								System.out.printf("\r%s\n", "The given file is stored on another node. Please wait while forwarding request to the right node...done.");
								StorageMessage responseForPut = StorageNode.pendingResponse.get(currentIdRequest);
								if(responseForPut != null){
									if(responseForPut.getReturnCode() == Message.ReturnCode.OK)
										System.out.println("The requested file has been saved in the remote node (" + ipRightBucket + ") successfully.");
									else
										System.out.println("A problem arise in the remote node performung put/update. Please try again.");
								} else
									System.err.println("The remote host did not receive the PUT request. Please retry.");
							}
						} else {
							File newFile = new File("./"+fileNameForPut);
							fileBA = Files.toByteArray(newFile);
							VectorClock newVC = new VectorClock();
							newVC.incrementVersion(StorageNode.myId.hashCode(), System.currentTimeMillis());
							Data newData = new Data(-1, false, "root", bucketForPut.get(0), fileNameForPut, "./files/", Base64.encodeBase64String(fileBA), newVC);
							int idUpdateBackupRequest = this.putData(newData);
						}
					} catch (IOException e1) {
						e1.printStackTrace();
						StorageNode.LOGGER.error("An error occurs while reading the given file. Path=./" + fileNameForPut);
					}
				} else 
					System.err.println("Error for PUT: please specify the name of the file you want to save in file system.");
				break;
			case "LIST":
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
					this.senderThread.addSendRequestToQueue(askForFileList, node.getHost());
				}
				this.executor.execute(this.senderThread);
				i = 0;
				attempts = 0;
				while(StorageNode.pendingRequest.size() > 0 && attempts < 20){
					System.out.printf("\r%s%c", "Please wait while retrieving file list from other nodes...", workchars[i]);
					try {
						sleep(250);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					i++;
					attempts++;
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
					if(listResponse != null){
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
					} else {
						System.err.println("One remote host did not receive the LIST request. Please retry.");
					}
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
				if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
					String fileNameForDelete = commandAndArgs.get(1);
					int idRequestDeleteBackup;
					try {
						idRequestDeleteBackup = this.deleteData(fileNameForDelete);
						this.executor.execute(this.senderThread);
						if(idRequestDeleteBackup != -1){
							// Wait for response
							i = 0;
							attempts = 0;
							while(StorageNode.pendingRequest.containsKey(idRequestDeleteBackup) && attempts < 20){
								System.out.printf("\r%s%c", "Please wait while processing your delete request...", workchars[i]);
								try {
									sleep(250);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								i++;
								if(i == workchars.length-1)
									i = 0;
							}
							System.out.printf("\r%s\n", "Please wait while processing your delete request...done.");
							StorageMessage remoteDeleteResponse = StorageNode.pendingResponse.get(idRequestDeleteBackup);
							if(remoteDeleteResponse != null){
								if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.OK)
									System.out.println("The given file has been successfully canceled.");
								else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.ERROR)
									System.out.println("The remote node encountered a problem while executing cancelation of the given file in replica node. Please try again.");
								else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.NOT_EXISTS)
									System.out.println("Error: the given file does not exists. Please check filename and try again.");
							} else
								System.err.println("The remote host did not receive the DELETE_BACKUP request. Please retry.");
						} else {
							StorageNode.LOGGER.info("The file requested for delete is not in this node. Retrieving remote host's infos.");
							ArrayList<String> bucketForThisData = StorageNode.retrieveBucketForMember(fileNameForDelete);
							if(bucketForThisData.size() > 1) {
								StorageNode.LOGGER.warn("More than one bucket for " + fileNameForDelete);
							} else if (!bucketForThisData.isEmpty()){
								String remoteip = this.getIPFromID(bucketForThisData.get(0));
								int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
								StorageMessage remoteDelMessage = new StorageMessage(StorageNode.myHost,
										remoteip,
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
								attempts = 0;
								while(StorageNode.pendingRequest.containsKey(currDeleteIdRequest) && attempts < 20){
									System.out.printf("\r%s%c", "Please wait while processing your delete request...", workchars[i]);
									try {
										sleep(250);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
									i++;
									attempts++;
									if(i == workchars.length-1)
										i = 0;
								}
								System.out.printf("\r%s\n", "Please wait while processing your delete request...done.");

								StorageMessage remoteDeleteResponse = StorageNode.pendingResponse.get(currDeleteIdRequest);
								if(remoteDeleteResponse != null){
									if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.OK)
										System.out.println("The given file has been successfully canceled.");
									else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.ERROR)
										System.out.println("The remote node encountered a problem while executing cancelation of the given file. Please try again.");
									else if(remoteDeleteResponse.getReturnCode() == Message.ReturnCode.NOT_EXISTS)
										System.out.println("Error: the given file does not exists. Please check filename and try again.");
								} else
									System.err.println("The remote host did not receive the DELETE request. Please retry.");
							} else {
								// This node is alone in the world. Delete this file here if it is present.
								if(!StorageNode.myFiles.containsKey(fileNameForDelete))
									System.err.print("The requested file is not present in the file system.");
								else
									System.out.println("The requested file is already present in your disk at: " + StorageNode.myFiles.get(fileNameForDelete).getPathToFile() + StorageNode.myFiles.get(fileNameForDelete).getFileName());
							}
						}
					} catch (IOException e1) {
						e1.printStackTrace();
						System.err.println(e1.getMessage());
					}
				} else 
					System.err.println("Error in DELETE: please specify the name of the file you want to delete.");
				break;
			case "HELP":
				break;
			case "QUIT":
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
		this.receiverThread.shutdown();
		this.senderThread.shutdown();
		this.executor.shutdown();
		try {
			boolean result = this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
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

	// PutData: the assumption is that this methods is called for update/put
	//		 only files that belongs to this bucket, otherwise return err.
	//		 P.S.: take care about replica copies
	// RETURNS: the id of the request for UPDATE_BACKUP (if the file is new), -1 otherwise
	private int putData(Data dataFile) throws IOException{
		int ret = -1;
		byte[] rcvdFile = null;
		File filesDir = null;
		if(!StorageNode.myFiles.containsKey(dataFile.getFileName())){
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
			if(!this.ipReplica.equals("")){
				int currentIDREQ = StorageNode.requestIDCounter.getAndIncrement();
				// Send file to replica node
				StorageMessage sendBackup = new StorageMessage(StorageNode.myHost,
						this.ipReplica,
						Message.Type.REQUEST, 
						currentIDREQ, 
						Message.Command.UPDATE_BACKUP, 
						-1, 
						null, 
						dataFile.getFileName(), 
						StorageNode.myFiles.get(dataFile.getFileName()), 
						null);
				StorageNode.pendingRequest.put(currentIDREQ, sendBackup);
				this.senderThread.addSendRequestToQueue(sendBackup, this.ipReplica);
				ret = currentIDREQ;
			}
		} else {
			// Update the given file here and in replica node
			//			for(String key : StorageNode.myFiles.keySet()){
			//				if(myFiles.get(dataFile.getFileName()).getFileName().equals(dataFile.getFileName())){
			VectorClock myCopy = myFiles.get(dataFile.getFileName()).getVersion();
			VectorClock newCopy = dataFile.getVersion();
			ArrayList<Integer> ids = new ArrayList<>();
			switch (myCopy.compare(newCopy)) {
			case AFTER:
				System.out.println("my vector clock is AFTER NEW CLOCK");
				// Do nothing
				break;
			case BEFORE:
				System.out.println("my vector clock is BEFORE new clock");
				// Update value and send update to replica node
				dataFile.setIdFile(myFiles.get(dataFile.getFileName()).getIdFile());
				myFiles.remove(dataFile.getFileName());
				myFiles.put(dataFile.getFileName(), dataFile);
				rcvdFile = Base64.decodeBase64(dataFile.getFile());
				// save the file to disk
				filesDir = new File(dataFile.getPathToFile());
				if(!filesDir.exists()){
					filesDir.mkdir();
				}
				File fileToSave = new File(dataFile.getPathToFile() + dataFile.getFileName());
				Files.write(rcvdFile, fileToSave);
				if(!this.ipReplica.equals("")){
					int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
					// Send updated value to backup copy
					StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
							this.ipReplica,
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
					this.responseHandlerThread.addIdsToHandle(ids);
					StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
					this.senderThread.addSendRequestToQueue(updateBackup, this.ipReplica);
				}
				break;
			case CONCURRENTLY:
				System.out.println("my vector clock is CONCURRENT to new clock");
				Data copyOfConflict = myFiles.get(dataFile.getFileName());
				copyOfConflict.addConflict(dataFile);
				myFiles.remove(dataFile.getFileName());
				myFiles.put(dataFile.getFileName(), copyOfConflict);
				int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
				// Send updated value to backup copy
				StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
						this.ipReplica,
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
				this.responseHandlerThread.addIdsToHandle(ids);
				StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
				this.senderThread.addSendRequestToQueue(updateBackup, this.ipReplica);
				break;
			}
		}
		//			}
		//		}
		return ret;
	}


	private int putBackupData(String idMaster, ArrayList<Data> backup) throws IOException{
		int ret = 0;
		System.out.println("Put backup for: " + idMaster + "  " + this.getIDFromIP(idMaster));
		if(!StorageNode.backupFiles.containsKey(idMaster)){
			StorageNode.backupFiles.put(idMaster, new HashMap<String, Data>());
		}
		for(Data data : backup){
			int tempRet = updateBackupData(idMaster, data, false);
			if(tempRet == -1){
				// CONFLICT
				ret = tempRet;
				System.err.println("PUT Backup conflict: " + data.getFileName());
			}
		}
		return ret;
	}

	/**
	 * 
	 * @param file
	 * @return 1 if the update was performed; 0 if the existent copy is updated; -1 in case of conflicts.
	 * @throws IOException 
	 */
	private int updateBackupData(String idMaster, Data file, boolean force) throws IOException{
		int ret = 0;
		if(!backupFiles.get(idMaster).containsKey(file.getFileName())){
			file.setReplica(true);
			HashMap<String, Data> currentBackup = StorageNode.backupFiles.get(idMaster); 
			StorageNode.backupFiles.remove(idMaster);
			currentBackup.put(file.getFileName(), file);
			StorageNode.backupFiles.put(idMaster, currentBackup);
			System.out.println("Backup files updated: " + StorageNode.backupFiles.get(idMaster).size());
			byte[] rcvdFile = Base64.decodeBase64(file.getFile());
			// save the file to disk
			File backupDir = new File("./backup");
			if(!backupDir.exists()){
				backupDir.mkdir();				
			}
			File fileToSave = new File("./backup/"+file.getFileName());
			Files.write(rcvdFile, fileToSave);
		} else {
			Data oldCopy = StorageNode.backupFiles.get(idMaster).get(file.getFileName());
			VectorClock myBackupVector = oldCopy.getVersion();
			VectorClock fileVector = file.getVersion();
			if(!force){
				HashMap<String, Data> currentBackupFiles = null;
				switch (myBackupVector.compare(fileVector)) {
				case AFTER:
					System.out.println("my vector clock is AFTER NEW CLOCK (backup)");
					// Do nothing
					break;
				case BEFORE:
					System.out.println("my vector clock is BEFORE NEW CLOCK (backup)");
					// Update value 
					file.setIdFile(oldCopy.getIdFile());
					currentBackupFiles = backupFiles.remove(idMaster);
					currentBackupFiles.put(file.getFileName(), file);
					StorageNode.backupFiles.put(idMaster, currentBackupFiles);
					ret = 1;
					break;
				case CONCURRENTLY:
					System.out.println("my vector clock is CONCURRENT to NEW CLOCK (backup)");
					file.setIdFile(oldCopy.getIdFile());
					file.addConflict(oldCopy);
					currentBackupFiles = backupFiles.remove(idMaster);
					currentBackupFiles.put(file.getFileName(), file);
					StorageNode.backupFiles.put(idMaster, currentBackupFiles);
					ret = -1;
					break;
				}
			} else {
				file.setIdFile(oldCopy.getIdFile());
				HashMap<String, Data> currentBackupFiles = backupFiles.remove(idMaster);
				currentBackupFiles.put(file.getFileName(), file);
				StorageNode.backupFiles.put(idMaster, currentBackupFiles);
				ret = 1;
			}

		}
		return ret;
	}

	private int deleteData(String fileName) throws IOException{
		int ret = -1;
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
		} else {
			cHasher.removeMember(fileName);
			Data deletedData = myFiles.remove(fileName);
			File fileToDelete = new File(deletedData.getPathToFile()  + deletedData.getFileName());
			if(fileToDelete.delete()){
				int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
				StorageMessage requestForDeleteBackup = new StorageMessage(StorageNode.myHost,
						this.ipReplica,
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
			} else
				throw new IOException("An error occurs while deleting file: " + deletedData.getPathToFile() + "/" + deletedData.getFileName());
		}
		return ret;
	}

	private JSONArray retrieveJSONListFiles(){
		JSONArray output = new JSONArray();
		StorageNode.myFiles.forEach((fileName, dataFile) -> output.put(dataFile.toJSONObject()));
		return output;
	}

	private int resolveConflictResolution(Data dataToUpdate, String senderIP){
		int ret = 0;
		byte[] rcvdFile = null;
		File filesDir = null;
		try {
			if(myFiles.containsKey(dataToUpdate.getFileName())){
				dataToUpdate.setIdFile(myFiles.get(dataToUpdate.getFileName()).getIdFile());
				VectorClock oldClock = myFiles.get(dataToUpdate.getFileName()).getVersion();
				VectorClock updatedClock = dataToUpdate.getVersion().merge(oldClock);
				dataToUpdate.setVersion(updatedClock);
				myFiles.remove(dataToUpdate.getFileName());
				myFiles.put(dataToUpdate.getFileName(), dataToUpdate);
				rcvdFile = Base64.decodeBase64(dataToUpdate.getFile());
				// save the file to disk
				filesDir = new File(dataToUpdate.getPathToFile());
				if(!filesDir.exists()){
					filesDir.mkdir();
				}
				File fileToSave = new File(dataToUpdate.getPathToFile() + dataToUpdate.getFileName());
				Files.write(rcvdFile, fileToSave);
				if(!this.ipReplica.equals("")){
					int idRequestForUpdateBackup = StorageNode.requestIDCounter.getAndIncrement();
					// Send updated value to backup copy
					StorageMessage updateBackup = new StorageMessage(StorageNode.myHost, 
							this.ipReplica,
							Message.Type.REQUEST,
							idRequestForUpdateBackup,
							Message.Command.CONFLICT_RESOLUTION,
							-1,
							null,
							dataToUpdate.getFileName(),
							StorageNode.myFiles.get(dataToUpdate.getFileName()),
							null);
					ret = idRequestForUpdateBackup;
					StorageNode.pendingRequest.put(idRequestForUpdateBackup, updateBackup);
					this.senderThread.addSendRequestToQueue(updateBackup, this.ipReplica);
				}
			} else if(backupFiles.containsKey(this.getIDFromIP(senderIP))){
				//				if(backupFiles.get(this.getIDFromIP(senderIP)).containsKey(dataToUpdate.getFileName()))
				this.updateBackupData(this.getIDFromIP(senderIP), dataToUpdate, true);
			} else
				StorageNode.LOGGER.warn("The given request for CONFLICT_RESOLUTION is wrong: no " + dataToUpdate.getFileName() + " file saved locally.");
		} catch (IOException e) {
			e.printStackTrace();
			StorageNode.LOGGER.error("An error occurred when saving file in local directory. " + dataToUpdate.getFileName());
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
				StorageMessage response = new StorageMessage(StorageNode.myHost,
						msg.getHost(),
						Message.Type.RESPONSE, 
						msg.getIdRequest(), 
						msg.getCommand(), 
						(output.getJSONObject(0).has("error")) ? Message.ReturnCode.NOT_EXISTS : Message.ReturnCode.OK, 
								output, 
								msg.getFileName(), 
								(output.getJSONObject(0).has("error")) ? null : dataForGet, 
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
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
				this.executor.execute(this.responseHandlerThread);
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
				int ret = this.deleteData(msg.getFileName());
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
						msg.getHost(),
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
				System.out.println("PUT_BACKUP: CurrLowerKey=" + StorageNode.cHasher.getLowerKey(StorageNode.myId) + " Arrives:" + this.getIDFromIP(msg.getHost()));
				if(StorageNode.cHasher.getLowerKey(StorageNode.myId).equals(this.getIDFromIP(msg.getHost()))){
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
				} else {
					putBackupResponse = new StorageMessage(StorageNode.myHost,
							msg.getHost(),
							Message.Type.RESPONSE,
							msg.getIdRequest(),
							msg.getCommand(),
							Message.ReturnCode.ACCEPTED_TO_CHECK,
							null,
							null,
							null,
							null);
				}
				this.senderThread.addSendRequestToQueue(putBackupResponse, msg.getHost());
				this.executor.execute(this.senderThread);
			} else if(type == Message.Type.RESPONSE){
				if(pendingRequest.containsKey(msg.getIdRequest())){
					pendingRequest.remove(msg.getIdRequest());
					pendingResponse.put(msg.getIdRequest(), msg);
					if(msg.getReturnCode() == Message.ReturnCode.ERROR){
						StorageNode.LOGGER.warn("A problem arise while send backup copies to " + msg.getHost() + " host.");
					} else { 
						if(msg.getReturnCode() == Message.ReturnCode.ACCEPTED_TO_CHECK){
							StorageNode.LOGGER.warn("Remote replica node has accepted my backup, but it has a different view of the network. Maybe we will have to send erase backup early.");
						} else
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
				this.senderThread.addSendRequestToQueue(responseForDeleteBackup, msg.getHost());
				this.executor.execute(senderThread);
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
				this.senderThread.addSendRequestToQueue(response, msg.getHost());
				this.executor.execute(this.senderThread);
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
				StorageNode.backupFiles.remove(this.getIDFromIP(msg.getHost()));
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
				this.senderThread.addSendRequestToQueue(eraseResponse, msg.getHost());
				this.executor.execute(this.senderThread);		
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
				int idRequestForUpdateBackup = this.resolveConflictResolution(msg.getData(), msg.getHost());
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
					this.senderThread.addSendRequestToQueue(conflictResponse, msg.getHost());
					this.executor.execute(this.senderThread);
				} else if (idRequestForUpdateBackup != 0){
					// Wait result with external thread for response handling
					ArrayList<Integer> ids = new ArrayList<>();
					ids.add(new Integer(idRequestForUpdateBackup));
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
					StorageNode.pendingRequest.put(idRequestForUpdateBackup, conflictResponse);
					this.senderThread.addSendRequestToQueue(conflictResponse, this.ipReplica);
					this.responseHandlerThread.addIdsToHandle(ids);
					this.executor.execute(senderThread);
					this.executor.execute(responseHandlerThread);
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
			e.printStackTrace();
			StorageNode.LOGGER.warn("The system isn't able to process a received message. It will be dropped: " + receivedMsg.toJSONObject().toString() + " : " + e.getMessage());
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
			if(member.getId().equals(this.idReplica)){
				// This case happens whenever the node which act as a replica for me goes down. 
				if(state == GossipState.DOWN){
					System.out.println("Dead member(REPLICANODE): " + member.toJSONObject());
					StorageNode.pendingRequest.forEach((idRequest, message) -> 
					{ 
						if(message.getHost().equals(member.getHost())){
							StorageNode.pendingRequest.remove(idRequest);
							if(StorageResponseAsyncHandler.idsToHandle.contains(idRequest))
								StorageResponseAsyncHandler.idsToHandle.remove(idRequest);
						}
					});
					StorageNode.cHasher.removeBucket(member.getId());
					String idNewPossibleReplica = StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId);
					System.out.println("Possible replica is: " + idNewPossibleReplica);
					// send PUT_BACKUP and be sure it receive the message (with separate thread?)
					if(!idNewPossibleReplica.equals(StorageNode.myId)){
						JSONArray backup = new JSONArray();
						StorageNode.myFiles.forEach((fileName, dataFile) -> backup.put(dataFile.toJSONObjectWithFile()));
						int idRequestForPutBackup = StorageNode.requestIDCounter.getAndIncrement();
						StorageMessage messageForNewPutBackup = new StorageMessage(StorageNode.myHost,
								this.getIPFromID(idNewPossibleReplica),
								Message.Type.REQUEST,
								idRequestForPutBackup,
								Message.Command.PUT_BACKUP,
								-1,
								null,
								null,
								null,
								backup);
						int i = 0;
						while(i < 2){
							StorageNode.pendingRequest.put(idRequestForPutBackup, messageForNewPutBackup);
							this.senderThread.addSendRequestToQueue(messageForNewPutBackup, this.getIPFromID(idNewPossibleReplica));
							this.executor.execute(this.senderThread);
							int attempts = 0;
							while(StorageNode.pendingRequest.containsKey(idRequestForPutBackup) && attempts < 20){
								try {
									sleep(250);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								attempts++;
							}
							StorageMessage responseForPutBackup = StorageNode.pendingResponse.get(idRequestForPutBackup);
							if(responseForPutBackup != null){
								if(responseForPutBackup.getReturnCode() == Message.ReturnCode.OK || responseForPutBackup.getReturnCode() == Message.ReturnCode.ACCEPTED_TO_CHECK){
									this.idReplica = idNewPossibleReplica;
									this.ipReplica = this.getIPFromID(this.idReplica); 
									StorageNode.LOGGER.info("Operation PUT_BACKUP in remote slave node completed.");
									break;
								} else {
									StorageNode.LOGGER.fatal("The PUT_REQUEST command fails in remote node " + this.getIPFromID(idNewPossibleReplica) + ". Till now the system will not guarantees complete fault-tolerance.");
									break;
								}
							} else if(i == 1){
								// Return error to user
								StorageNode.LOGGER.error("The system encountered a problem while sending new backup request to remote node: " + this.getIPFromID(idNewPossibleReplica) +
										". We tried to re-enstablish a stable situation without success. Till now the system will not guarantees complete fault-tolerance until this operation isn't completed.");

							}
							i++;
						}

					} else {
						this.ipReplica = "";
						this.idReplica = "";
						StorageNode.LOGGER.info("This node is the only one in this distributed file system network.");
					}
				}
			} else if(member.getId().equals(this.idPrecNode)){
				// This is the case happens whenever the node for which i act as replica, goes down.
				if(state == GossipState.DOWN){
					System.out.println("Dead member(PRECNODE): " + member.toJSONObject());
					StorageNode.cHasher.removeBucket(member.getId());
					StorageNode.pendingRequest.forEach((idRequest, message) -> 
					{ 
						if(message.getHost().equals(member.getHost())){
							StorageNode.pendingRequest.remove(idRequest);
							if(StorageResponseAsyncHandler.idsToHandle.contains(idRequest))
								StorageResponseAsyncHandler.idsToHandle.remove(idRequest);
						}
					});
					this.idPrecNode = StorageNode.cHasher.getLowerKey(StorageNode.myId);
					for(LocalGossipMember memb: grs.getGossipService().get_gossipManager().getMemberList()){
						if(memb.getId() == this.idPrecNode)
							this.ipPrecNode = memb.getHost();
					}
					// Now pass your backup file in myFiles and then ask for backup to your new node for which act as replica
					HashMap<String, Data> backupFromDeadMember = StorageNode.backupFiles.get(member.getId()); 
					StorageNode.backupFiles.remove(member.getId());
					if(backupFromDeadMember != null){
						ArrayList<Integer> ids = new ArrayList<>();
						backupFromDeadMember.forEach((filename, data) -> 
						{
							System.out.println("Storing data replica: "  + data.getFileName());
							StorageNode.myFiles.put(data.getFileName(), data);
							StorageNode.cHasher.addMember(data.getFileName());
							int idRequestUpdate = StorageNode.requestIDCounter.getAndIncrement();
							StorageMessage msgupdate = new StorageMessage(
									StorageNode.myHost,
									this.ipPrecNode,
									Message.Type.REQUEST,
									idRequestUpdate,
									Message.Command.UPDATE_BACKUP,
									-1,
									null,
									data.getFileName(),
									data,
									null);
							ids.add(idRequestUpdate);
							StorageNode.pendingRequest.put(idRequestUpdate, msgupdate);
							this.senderThread.addSendRequestToQueue(msgupdate, this.ipReplica);
						});
						this.responseHandlerThread.addIdsToHandle(ids);
						this.executor.execute(this.senderThread);
						this.executor.execute(this.responseHandlerThread);
					} else {
						this.idPrecNode = "";
						this.ipPrecNode = "";
					}
				}
			} else {
				// Generic node case
				if(state == GossipState.DOWN){
					System.out.println("Dead member: " + member.toJSONObject());
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
				} else {
					System.out.println("NEW Member: " + member.toJSONObject());
					// Check if this node has some data to send to new node.
					StorageNode.cHasher.addBucket(member.getId());
					ArrayList<Data> setDataForSendUpdate = new ArrayList<Data>();
					StorageNode.myFiles.forEach((filename, data) -> 
					{
						ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(filename);
						if(bucketFor.get(0).equals(member.getId())){ 
							setDataForSendUpdate.add(data);
							// TODO: remove only when you receive response from next PUT + cancel from disk
							StorageNode.myFiles.remove(filename);
						}
					});
					ArrayList<Integer> ids = new ArrayList<>();
					setDataForSendUpdate.forEach(data -> 
					{
						int idForUpdate = StorageNode.requestIDCounter.getAndIncrement();
						int idForDeleteBackup = StorageNode.requestIDCounter.getAndIncrement();
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
						ids.add(idForUpdate);
						StorageMessage deleteBackupFile = new StorageMessage(
								StorageNode.myHost,
								this.ipReplica,
								Message.Type.REQUEST,
								idForDeleteBackup,
								Message.Command.DELETE_BACKUP,
								-1,
								null,
								data.getFileName(),
								null,
								null);
						this.responseHandlerThread.addIdsToHandle(ids);
						this.senderThread.addSendRequestToQueue(updateFile, member.getHost());
						this.senderThread.addSendRequestToQueue(deleteBackupFile, ipReplica);
						StorageNode.pendingRequest.put(idForDeleteBackup, deleteBackupFile);
						StorageNode.pendingRequest.put(idForUpdate, updateFile);
					});
					this.executor.execute(this.senderThread);
					this.executor.execute(this.responseHandlerThread);
					// Wait response. Retry two times in case of not receiving response.
					// check if this node is really new Prec node and wait PUT_BACKUP
					if(member.getId().equals(StorageNode.cHasher.getLowerKey(myId))){
						this.idPrecNode = StorageNode.cHasher.getLowerKey(StorageNode.myId);
						for(LocalGossipMember memb: grs.getGossipService().get_gossipManager().getMemberList()){
							if(memb.getId() == this.idPrecNode)
								this.ipPrecNode = memb.getHost();
						}
					} else if(member.getId().equals(StorageNode.cHasher.getDescendantBucketKey(myId))){
						// This is the case whenever new node replica born. PUT_BACKUP in new node and wait result: if ok then ERASE_BACKUP else RETRY
						String idNewPossibleReplica = StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId);
						// send PUT_BACKUP and be sure it receive the message (with separate thread?)
						JSONArray backup = new JSONArray();
						StorageNode.myFiles.forEach((fileName, dataFile) -> backup.put(dataFile.toJSONObjectWithFile()));
						int idRequestForPutBackup = StorageNode.requestIDCounter.getAndIncrement();
						StorageMessage messageForNewPutBackup = new StorageMessage(StorageNode.myHost,
								this.getIPFromID(idNewPossibleReplica),
								Message.Type.REQUEST,
								idRequestForPutBackup,
								Message.Command.PUT_BACKUP,
								-1,
								null,
								null,
								null,
								backup);
						int i = 0;
						while(i < 2){
							StorageNode.pendingRequest.put(idRequestForPutBackup, messageForNewPutBackup);
							this.senderThread.addSendRequestToQueue(messageForNewPutBackup, this.getIPFromID(idNewPossibleReplica));
							this.executor.execute(this.senderThread);
							int attempts = 0;
							while(StorageNode.pendingRequest.containsKey(idRequestForPutBackup) && attempts < 20){
								try {
									sleep(250);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								attempts++;
							}
							StorageMessage responseForPutBackup = StorageNode.pendingResponse.get(idRequestForPutBackup);
							if(responseForPutBackup != null){
								if(responseForPutBackup.getReturnCode() == Message.ReturnCode.OK || responseForPutBackup.getReturnCode() == Message.ReturnCode.ACCEPTED_TO_CHECK){
									int idForEraseBackup = StorageNode.requestIDCounter.getAndIncrement();
									StorageMessage messageForEraseBackup = new StorageMessage(StorageNode.myHost,
											this.ipReplica,
											Message.Type.REQUEST,
											idForEraseBackup,
											Message.Command.ERASE_BACKUP,
											-1,
											null,
											null,
											null,
											null);
									this.senderThread.addSendRequestToQueue(messageForEraseBackup, ipReplica);
									StorageNode.pendingRequest.put(idForEraseBackup, messageForEraseBackup);
									this.executor.execute(this.senderThread);
									this.idReplica = idNewPossibleReplica;
									this.ipReplica = this.getIPFromID(this.idReplica); 
									StorageNode.LOGGER.info("Operation PUT_BACKUP in remote slave node completed. IP=" + this.ipReplica + " ID=" + this.idReplica);
									break;
								} else {
									StorageNode.LOGGER.fatal("The PUT_REQUEST command fails in remote node " + this.getIPFromID(idNewPossibleReplica) + ". Till now the system will not guarantees complete fault-tolerance.");
									break;
								}
							} else if(i == 1){
								// check if the node that didn't receive the packet is dead.
								if(!StorageNode.cHasher.getDescendantBucketKey(StorageNode.myId).equals(idNewPossibleReplica)){
									// Return error to user
									StorageNode.LOGGER.error("The system encountered a problem while sending new backup request to remote node: " + idNewPossibleReplica + " " + this.getIPFromID(idNewPossibleReplica) +
											". We tried to re-enstablish a stable situation without success. Till now the system will not guarantees complete fault-tolerance until this operation isn't completed.");
								}
							}
							i++;
						}
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			StorageNode.LOGGER.error("An error occurred while deteting bucket: " + member.getHost() + ":" + member.getId() + ".");
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
						this.senderThread.addSendRequestToQueue(thismessage, thismessage.getDestinationHost());
						newIds.add(newIdForResend);
						StorageNode.pendingRequest.put(newIdForResend, thismessage);
					} else
						StorageNode.LOGGER.info("The message with id " + id + " for " + StorageNode.pendingRequest.get(id).getDestinationHost() + " encountered an error during delivery. " + StorageNode.pendingRequest.get(id).toJSONObject());
				} else
					StorageNode.LOGGER.info("The message with id " + id + " does not exists in pending request. This tells that something goes wrong...");
			});
			StorageResponseAsyncHandler.idsToHandle.clear();
			if(!newIds.isEmpty()){
				this.responseHandlerThread.addIdsToHandle(newIds);
				this.executor.execute(senderThread);
				this.executor.execute(responseHandlerThread);
			}
		}
	}

}
