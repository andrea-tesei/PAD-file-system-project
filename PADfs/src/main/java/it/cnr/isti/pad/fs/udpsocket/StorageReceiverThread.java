package it.cnr.isti.pad.fs.udpsocket;

import java.util.concurrent.atomic.AtomicBoolean;

abstract public class StorageReceiverThread implements Runnable {

	private UDPServer udpServer = null;
	
	private AtomicBoolean keepRunning;
	
	@Override
	public void run() {
		
		
	}
	
	abstract protected StorageMessage receiveMessage();

}
