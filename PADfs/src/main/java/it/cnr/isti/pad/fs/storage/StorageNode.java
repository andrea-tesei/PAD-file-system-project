package it.cnr.isti.pad.fs.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.event.OnMessageReceivedListener;
import it.cnr.isti.pad.fs.event.OnSendMessageListener;
import it.cnr.isti.pad.fs.gossiping.GossipResourceService;

/** 
 * StorageNode class: it implements the logic of the storage functions of each node of the net.
 * 					  It also implement the GossipListener interface in order to listen for
 * 					  new UP/DOWN nodes.
 * */
public class StorageNode extends Thread implements GossipListener, OnMessageReceivedListener {

	private static int localMembersIDCounter = 0;
	
	private static int requestIDCounter = 0;
	
	// The files for which this node is responsible.
	private static HashMap<Integer, Data> myFiles;
	
	// The backup files for which this node acts as replica.
	private static HashMap<Integer, Data> backupFiles;
	
	// The Consistent Hasher instance
	private static ConsistentHasher<String, String> cHasher;
	
	// Array of pending requests
	private static ArrayList<StorageMessage> pendingRequest;
	
	// Instance of Gossip Service
	public static GossipResourceService grs = null;

	// Listener for send message
	private static OnSendMessageListener listener = null; 
	
	public StorageNode(File fileSettings){
		myFiles = new HashMap<Integer, Data>();
		backupFiles = new HashMap<Integer, Data>();
		pendingRequest = new ArrayList<StorageMessage>();
		grs = new GossipResourceService(fileSettings, this);
		cHasher = new ConsistentHasherImpl<>(
				this.logBase2(grs.getGossipService().get_gossipManager().getMemberList().size(), 2),
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.SHA1);
		// TODO: the way waiting for MemberList has to be changed.
		while(grs.getGossipService().get_gossipManager().getMemberList().isEmpty())
			;
		grs.getGossipService().get_gossipManager().getMemberList().forEach(member -> cHasher.addBucket(member.getId()));
	}
	
	// Auxiliary function: calculate the logarithm of the given "input" with the given "base"
	public int logBase2(int input, int base){
		return (int) (Math.log(input) / Math.log(base));
	}
	
	// Auxiliary function: retrieve the backet's id corresponding to the given member.
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
	
	// TODO: check version increment
	public static void putData(Data file){
		// Check if this file has to be stored in this Bucket or another
		ArrayList<String> bucketForFile = StorageNode.retrieveBucketForMember(file.getFileName());
		String bucket = "";
		if(bucketForFile.size() == 1)
			bucket = bucketForFile.get(0);
		else
			App.LOGGER.warn("More than one bucket for " + file.getFileName());
		if(!bucket.equals(grs.getGossipService().get_gossipManager().getMyself().getId())){
			// TODO: Forward request to right node
		} else {
			// Check if this file already exists
			Boolean trovato = false;
			for(String member : cHasher.getAllMembers()){
				if(member.equals(file.getFileName()))
					trovato = true;
			}
			if(trovato == false){
				StorageNode.myFiles.put(StorageNode.localMembersIDCounter, file);
				StorageNode.localMembersIDCounter++;
				StorageNode.cHasher.addMember(file.getFileName());
				// TODO: Send file to replica node
			} else {
				// Update the given file
				StorageNode.myFiles.keySet().forEach(key ->
				{
					if(myFiles.get(key).getFileName().equals(file.getFileName())){
//						file.setVersion(myFiles.get(key).getVersion().incremented(nodeId, time));
						myFiles.put(key, file);
					}
				});
			}
		}
	}
	
//	private ThreadListener listener = null;
//
//	   public void addListener(ThreadListener listener) {
//	      this.listener = listener;
//	   }
//
//	   private void informListener() {
//	      if (listener != null) {
//	      }
//	         listener.onNewData("Hello from " + this.getName());
	
	public static void addListener(OnSendMessageListener listen){
		StorageNode.listener = listen;
	}
	
	private void triggerListeners(StorageMessage msg){
		if(listener != null)
			listener.onRequestSendMessage(msg);
	}
	
	public static Data getData(Integer idFile){
		return new Data();
	}
	
	public static ArrayList<Data> getListFiles(){
		return new ArrayList<Data>();
	}
	
	public static boolean resolveConflict(Integer idFile){
		return true;
	}
	
	// Method for processing Message request from other nodes
	public static void processRequest(StorageMessage msg){
		
	}
	
	@Override
	public StorageMessage onReceivedMessage() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void gossipEvent(GossipMember member, GossipState state) {
		// If the state is down: send member's data to the new copy_replica and cleanup the env
		// If the state is up: put it in the env and rebalance all the CH's ring.
		System.out.println("bleeeeeeeeee listeneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		super.run();
	}


}
