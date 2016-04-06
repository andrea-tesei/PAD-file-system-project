package it.cnr.isti.pad.PADfs;

import java.io.File;
import it.cnr.isti.pad.Gossiping.GossipResourceService;
import it.cnr.isti.pad.UDPSocket.UDPServer;

public class App 
{
	public static GossipResourceService grs = null;
	public static UDPServer udpServer = null;
	
    public static void main( String[] args )
    {
    	
    	// Instanciation of Gossip Runner instance
    	org.apache.log4j.BasicConfigurator.configure();
	
		File configFile = null;

		if (args.length == 1) {
			configFile = new File("./" + args[0]);
			App.grs = new GossipResourceService(configFile);
			App.udpServer = new UDPServer();
		} else {
			System.err.println("Error: settings file is missing. You must specify a configuration file.");
			return;
		}
			
    }
}
