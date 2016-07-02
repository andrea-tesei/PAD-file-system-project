package it.cnr.isti.pad.fs.runnables;

import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import it.cnr.isti.pad.fs.entry.App;
import it.cnr.isti.pad.fs.storage.StorageNode;
import it.cnr.isti.pad.fs.udpsocket.SocketRemoteInfo;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.UDPClientsHandler;

abstract public class StorageSenderThread implements Runnable {

	protected UDPClientsHandler clientsHandlerSocket = null; 
	
	public static final Logger LOGGER = Logger.getLogger(StorageSenderThread.class);

	protected ConcurrentHashMap<String, ArrayList<StorageMessage>> pendingSendRequest = null;
	
	public StorageSenderThread(){
		try {
			clientsHandlerSocket = new UDPClientsHandler();
			pendingSendRequest = new ConcurrentHashMap<String,ArrayList<StorageMessage>>();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		StorageSenderThread.LOGGER.assertLog(pendingSendRequest != null, "The send message queue is null.");
		pendingSendRequest.forEach((ip,requestlist) -> 
		{ 
			SocketAddress addressForSend = clientsHandlerSocket.getSocketAddressFromIP(ip);
			if(addressForSend != null) {
				requestlist.forEach(request ->
				{
					try {
						StorageSenderThread.LOGGER.info("Sending message to " + ip + " " + request.toJSONObject().toString());
						clientsHandlerSocket.sendPacket(request.toJSONObject().toString().getBytes(), addressForSend);
					} catch (Exception e) {
						System.err.println("The given file is too large. Please respect the maximum size of 65,507 bytes.");
						StorageSenderThread.LOGGER.error("Error while parsing send request message to: " + ip + "; Error: " + e.getMessage());
					}
				});
			} else
				StorageSenderThread.LOGGER.error("The specified address is not bounded to any client. " + ip);
		});
		pendingSendRequest.clear();
	}

	public void shutdown(){
		clientsHandlerSocket.closeConnection();
		clientsHandlerSocket = null;
	}
	
	public HashMap<String, SocketRemoteInfo> getClientsHosts(){
		return clientsHandlerSocket.getNodes();
	}

	abstract protected void processSendMessageRequests(); 
	
	abstract protected void addSendRequestToQueue(StorageMessage msg, String ip);

}
