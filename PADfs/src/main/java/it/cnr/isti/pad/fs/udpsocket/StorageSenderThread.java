package it.cnr.isti.pad.fs.udpsocket;

import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import it.cnr.isti.pad.fs.entry.App;

abstract public class StorageSenderThread implements Runnable {

	protected UDPClientsHandler clientsHandlerSocket = null; 

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
		App.LOGGER.assertLog(pendingSendRequest != null, "The send message queue is null.");
		pendingSendRequest.forEach((ip,requestlist) -> 
		{ 
			SocketAddress addressForSend = clientsHandlerSocket.getSocketAddressFromIP(ip);
			if(addressForSend != null) {
				requestlist.forEach(request ->
				{
					try {
						App.LOGGER.info("Sending message to " + ip + " " + request.toJSONObject().toString());
						clientsHandlerSocket.sendPacket(request.toJSONObject().toString().getBytes(), addressForSend);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						App.LOGGER.error("Error while parsing send request message to: " + ip);
					}
				});
			} else
				App.LOGGER.error("The specified address is not bounded to any client. " + ip);
		});
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
