package it.cnr.isti.pad.fs.udpsocket;

import java.util.concurrent.atomic.AtomicBoolean;

abstract public class StorageSenderThread implements Runnable {

	protected UDPClientsHandler clientsHandlerSocket = null; 
	
	protected AtomicBoolean keepRunning;
	
	@Override
	public void run() {
		/*
		 * TODO: it has to be the handler for send request from StorageNode
		 */
	}
	
	abstract protected void sendMessage(StorageMessage msg); 

}
