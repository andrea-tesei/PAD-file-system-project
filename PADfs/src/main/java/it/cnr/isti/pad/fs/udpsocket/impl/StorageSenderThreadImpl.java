package it.cnr.isti.pad.fs.udpsocket.impl;

import java.util.ArrayList;

import it.cnr.isti.pad.fs.runnables.StorageSenderThread;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

public class StorageSenderThreadImpl extends StorageSenderThread {
	
	public StorageSenderThreadImpl(){
		super();
	}
	
	@Override
	protected void processSendMessageRequests() {
		super.run();
	}
	
	public void refreshClientsSKT(){
		this.clientsHandlerSocket.refreshClientsSocketList();
	}


	@Override
	public void addSendRequestToQueue(StorageMessage msg, String ip) {
		ArrayList<StorageMessage> msglist = new ArrayList<StorageMessage>();
		msglist.add(msg);
		// Add message to the pending list for the destination's address
		if(pendingSendRequest.containsKey(ip)){
			pendingSendRequest.get(ip).forEach(request -> msglist.add(request));
			pendingSendRequest.remove(ip);
			pendingSendRequest.put(ip, msglist);
		} else
			pendingSendRequest.put(ip,msglist);
	}

}
