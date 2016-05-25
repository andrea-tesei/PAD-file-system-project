package it.cnr.isti.pad.fs.udpsocket.impl;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.code.gossip.manager.GossipManager;

import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.event.OnMessageReceivedListener;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.StorageReceiverThread;
import it.cnr.isti.pad.fs.udpsocket.UDPServer;

public class StorageReceiverThreadImpl extends StorageReceiverThread {

	private OnMessageReceivedListener listener = null;
	
	public StorageReceiverThreadImpl() throws UnknownHostException, SocketException{
		super();
	}
	
	public void addListener(OnMessageReceivedListener listen){
		this.listener = listen;
	}
	
	private void triggerListeners(StorageMessage msg){
		if(listener != null)
			listener.onReceivedMessage(msg);
	}
	
	@Override
	protected void processMessage(JSONObject receivedMsg) {
		// TODO Auto-generated method stub
		try {
			triggerListeners(new StorageMessage(receivedMsg));
		} catch (JSONException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			StorageReceiverThread.LOGGER.error("Error while receiving message: " + receivedMsg.toString() + "; Error: " + e.getMessage());
		}
	}

}
