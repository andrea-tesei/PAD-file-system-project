package it.cnr.isti.pad.fs.udpsocket;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageReceiverThreadImpl;

abstract public class StorageReceiverThread implements Runnable {

	protected UDPServer udpServer = null;
	
	protected AtomicBoolean keepRunning;
	
	public static final Logger LOGGER = Logger.getLogger(StorageReceiverThread.class);
	
	public StorageReceiverThread() throws UnknownHostException, SocketException{
		keepRunning = new AtomicBoolean(true);
		udpServer = new UDPServer();
		
	}
	
	@Override
	public void run() {
		while (keepRunning.get()) {
			try {
				JSONObject receivedPacket = this.udpServer.receivePacket(); 
				StorageReceiverThread.LOGGER.info("Received message: " + receivedPacket);
				this.processMessage(receivedPacket);
			} catch (JSONException e) {
				e.printStackTrace();
				StorageReceiverThread.LOGGER.error("Error while processing received message. Error: " + e.getMessage());
			}
		}
	}
	
	public String getServerHost(){
		return udpServer.getServerName();
	}
	
	public void shutdown(){
		keepRunning.set(false);
		this.udpServer.closeConnection();
		this.udpServer = null;
		StorageReceiverThread.LOGGER.info("The receiver thread is shutting down..");
	}
	
	abstract protected void processMessage(JSONObject receivedMsg);

}
