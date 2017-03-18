package it.cnr.isti.pad.fs.runnables;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.udpsocket.UDPServer;
import it.cnr.isti.pad.fs.udpsocket.impl.StorageReceiverThreadImpl;

/**
 * StorageReceiverThread class.
 * It implements a Runnable which acts as a listener thread for all received messages.
 * 
 * @author Andrea Tesei
 *
 */
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
			} catch (IOException e) {
				if(e.getMessage().equals("Socket closed"))
					StorageReceiverThread.LOGGER.info("The socket is closed.");
				else
					e.printStackTrace();
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
