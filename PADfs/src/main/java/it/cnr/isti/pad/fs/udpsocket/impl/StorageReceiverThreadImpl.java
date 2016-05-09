package it.cnr.isti.pad.fs.udpsocket.impl;

import it.cnr.isti.pad.fs.event.OnMessageReceivedListener;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.StorageReceiverThread;

public class StorageReceiverThreadImpl extends StorageReceiverThread {

	private OnMessageReceivedListener listener = null;
	
	public void addListener(OnMessageReceivedListener listen){
		this.listener = listen;
	}
	
	private void triggerListeners(){
		if(listener != null)
			listener.onReceivedMessage();
	}
	
	@Override
	protected StorageMessage receiveMessage() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void run(){
		
	}

}
