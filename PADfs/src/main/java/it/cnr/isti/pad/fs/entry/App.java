package it.cnr.isti.pad.fs.entry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import com.google.common.io.Files;

import it.cnr.isti.pad.fs.storage.StorageNode;
import it.cnr.isti.pad.fs.udpsocket.UDPClientsHandler;
import it.cnr.isti.pad.fs.udpsocket.UDPServer;

public class App 
{
	public static final Logger LOGGER = Logger.getLogger(App.class);
	public static UDPServer udpServer = null;
	public static UDPClientsHandler clientsHandlerSocket = null; 
	
	
	
    public static void main( String[] args )
    {
    
    	// Instanciation of Gossip Service
    	org.apache.log4j.BasicConfigurator.configure();
	
		File configFile = null;

		if (args.length == 1) {
			configFile = new File("./" + args[0]);
			//App.grs = new GossipResourceService(configFile);
			StorageNode node = new StorageNode(configFile);
			ArrayList<String> bucketFor = node.retrieveBucketForMember("prova.txt");
			bucketFor.forEach(bucket -> System.out.println("BucketFor: " + bucket));
			
			
			
//			try {
//				App.udpServer = new UDPServer();
//				App.clientsHandlerSocket = new UDPClientsHandler();
				UDPClientRunnable udpClientRunnable = new UDPClientRunnable();
				UDPServerRunnable udpServerRunnable = new UDPServerRunnable();
				Thread udpclientThread = new Thread(udpClientRunnable);
				Thread udpserverThread = new Thread(udpServerRunnable);
//				udpclientThread.run();
//				udpserverThread.run();
//			} catch (UnknownHostException e) {
//				e.printStackTrace();
//				grs.getGossipService().get_gossipManager().shutdown();
//				App.LOGGER.error("Error: a problem arise while extracting your address. Please check it and try again.");
//			} catch (SocketException e) {
//				e.printStackTrace();
//				grs.getGossipService().get_gossipManager().shutdown();
//				App.LOGGER.error("Error: a problem arise while setting up the server socket. Please try again later.");
//			}
		} else {
			System.err.println("Error: settings file is missing. You must specify a configuration file.");
			return;
		}
			
    }
    
    public static class UDPClientRunnable implements Runnable{

		@Override
		public void run() {
			JSONObject sendMsg = new JSONObject();
			try {
				File file = new File("./Coupon.pdf");
				sendMsg.put("host", App.udpServer.getServerName());
				sendMsg.put("type", "REQUEST");
				sendMsg.put("command", "GET");
				byte[] provaFileBA = Files.toByteArray(file);
				sendMsg.put("filename", file.getName());
				sendMsg.put("file",Base64.encodeBase64String(provaFileBA));
			} catch (JSONException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				App.LOGGER.error("The system encountered a problem while packeting your message. Please try again later.");
			} 
			App.clientsHandlerSocket.getNodes().forEach((ip,info) -> App.clientsHandlerSocket.sendPacket(sendMsg.toString().getBytes(), info.getRemoteServerAddr()));
					
		}
    	
    }
    
    public static class UDPServerRunnable implements Runnable{
    	
    	@Override
		public void run() {
    		
    		while(true){ // trova modo piu furbo di controllare il thread
    			JSONObject rcvdJson = null;
    			String host = "";
    			String receivedPacket = App.udpServer.receivePacket();
    			try {
    				rcvdJson = new JSONObject(receivedPacket);
    				host = rcvdJson.getString("host");
    				byte[] rcvdFile = Base64.decodeBase64(rcvdJson.getString("file"));
    				// save the file to disk
    				File saveFile = new File("./gossipzzz.pdf");
    				Files.write(rcvdFile, saveFile);
    				App.LOGGER.info("Received packet from: " + host + "  :  " + rcvdJson.getString("type") + " " + rcvdJson.getString("command")); // new String(rcvpacket.getData(), rcvpacket.getOffset(), rcvpacket.getLength(), "UTF-8")
    			} catch (JSONException | IOException e) {
    				e.printStackTrace();
    				App.LOGGER.error("The system encountered a problem while processing received json. Packet from: " + host);
    			}
    		}
		}
    }
}
