package it.cnr.isti.pad.UDPSocket;

import java.net.InetAddress;
import java.net.UnknownHostException;

import it.cnr.isti.pad.PADfs.App;

public class UDPServer {
	private final int localPort = 9099;
	private String serverName = "";
	
	public UDPServer(){
		try {
			this.serverName = InetAddress.getLocalHost().getHostAddress();
			System.out.println("Starting UDP server at: " + this.serverName);
			if(App.grs != null){
				App.grs.getGossipService().get_gossipManager().getMemberList().forEach(node -> System.out.println(node.getAddress()));
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
