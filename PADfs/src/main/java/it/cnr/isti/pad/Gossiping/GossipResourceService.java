package it.cnr.isti.pad.Gossiping;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;

import org.json.JSONException;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.StartupSettings;

public class GossipResourceService {

	private static File fileSettings = null;
	
	private GossipService gossipService = null;
	
	public GossipService getGossipService() {
		return gossipService;
	}

	public GossipResourceService(File fileSettings){
		GossipResourceService.fileSettings = fileSettings;
		if (fileSettings != null && fileSettings.exists()) {
			try {
				System.out.println("Parsing the configuration file...");
				StartupSettings _settings = StartupSettings.fromJSONFile(fileSettings);
				this.gossipService = new GossipService(_settings);
				System.out.println("Gossip service successfully initialized, let's start it...");
				gossipService.start();
			} catch (FileNotFoundException e) {
				System.err.println("The given file is not found!");
			} catch (JSONException e) {
				System.err.println("The given file is not in the correct JSON format!");
			} catch (IOException e) {
				System.err.println("Could not read the configuration file: " + e.getMessage());
			} catch (InterruptedException e) {
				System.err.println("Error while starting the gossip service: " + e.getMessage());
			}
		} else {
			System.out.println(
					"The gossip.conf file is not found.\n\nEither specify the path to the startup settings file or place the gossip.conf file in the same folder as the JAR file.");
		}
	}
	
	
}
