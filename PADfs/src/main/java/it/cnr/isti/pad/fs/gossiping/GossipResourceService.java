package it.cnr.isti.pad.fs.gossiping;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;
import org.json.JSONException;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.StartupSettings;
import com.google.code.gossip.event.GossipListener;

import it.cnr.isti.pad.fs.storage.StorageNode;

public class GossipResourceService {

	private static File fileSettings = null;
	
	private GossipService gossipService = null;
	
	public static final Logger LOGGER = Logger.getLogger(GossipResourceService.class);
	
	public GossipService getGossipService() {
		return gossipService;
	}

	public GossipResourceService(File fileSettings, GossipListener listener){
		GossipResourceService.fileSettings = fileSettings;
		if (fileSettings != null && fileSettings.exists()) {
			try {				
				GossipResourceService.LOGGER.info("Parsing the configuration file...");
				StartupSettings _settings = StartupSettings.fromJSONFile(fileSettings);
				this.gossipService = new GossipService(InetAddress.getByName(_settings.getHostname()).getHostAddress(), _settings.getPort(), _settings.getId(), _settings.getGossipMembers(), _settings.getGossipSettings(), listener);
				GossipResourceService.LOGGER.info("Gossip service successfully initialized, let's start it...");
				gossipService.start();
			} catch (FileNotFoundException e) {
				GossipResourceService.LOGGER.error("The given file is not found!");
			} catch (JSONException e) {
				GossipResourceService.LOGGER.error("The given file is not in the correct JSON format!");
			} catch (IOException e) {
				GossipResourceService.LOGGER.error("Could not read the configuration file: " + e.getMessage());
			} catch (InterruptedException e) {
				GossipResourceService.LOGGER.error("Error while starting the gossip service: " + e.getMessage());
			}
		} else {
			GossipResourceService.LOGGER.info("The gossip.conf file is not found.\nEither specify the path to the startup settings file or place the gossip.conf file in the same folder as the JAR file.");
		}
	}
	
	
}
