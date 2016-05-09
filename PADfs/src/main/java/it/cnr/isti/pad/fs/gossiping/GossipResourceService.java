package it.cnr.isti.pad.fs.gossiping;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import org.json.JSONException;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.StartupSettings;
import com.google.code.gossip.event.GossipListener;

public class GossipResourceService {

	private static File fileSettings = null;
	
	private GossipService gossipService = null;
	
	public GossipService getGossipService() {
		return gossipService;
	}

	public GossipResourceService(File fileSettings, GossipListener listener){
		GossipResourceService.fileSettings = fileSettings;
		if (fileSettings != null && fileSettings.exists()) {
			try {
				/* 
				 *  public GossipService(String ipAddress, int port, String id,
          List<GossipMember> gossipMembers, GossipSettings settings, GossipListener listener)
          
          
          InetAddress.getByName(startupSettings.getHostname()).getHostAddress(), startupSettings.getPort(), startupSettings.getId(),
             startupSettings.getGossipMembers(), startupSettings
                    .getGossipSettings(), null
				 * */
				
				
				System.out.println("Parsing the configuration file...");
				StartupSettings _settings = StartupSettings.fromJSONFile(fileSettings);
//				this.gossipService = new GossipService(_settings);
				this.gossipService = new GossipService(InetAddress.getByName(_settings.getHostname()).getHostAddress(), _settings.getPort(), _settings.getId(), _settings.getGossipMembers(), _settings.getGossipSettings(), listener);
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
