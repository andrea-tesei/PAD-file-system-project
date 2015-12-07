package com.google.code.gossip;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;

import org.json.JSONException;

public class GossipRunner 
{
	public static void main(String[] args) 
	{
		org.apache.log4j.BasicConfigurator.configure();
		
		File configFile = null;

		if (args.length == 0)
			configFile = new File("gossip.conf");
		else if (args.length == 1)
			configFile = new File("./" + args[0]);
		
		if (configFile != null)
			new GossipRunner(configFile);
		else
			new GossipRunner(Integer.parseInt(args[0]), args[1]);
	}

	public GossipRunner(File configFile) 
	{

		if (configFile != null && configFile.exists()) {
			try {
				System.out.println("Parsing the configuration file...");
				StartupSettings _settings = StartupSettings.fromJSONFile(configFile);
				GossipService gossipService = new GossipService(_settings);
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
	
	public GossipRunner(final int port, String seed)
	{
		try {
			StartupSettings settings = new StartupSettings(port, LogLevel.DEBUG);
			settings.addGossipMember(new RemoteGossipMember(seed, port, ""));
			GossipService gossipService = new GossipService(settings);
			System.out.println("Gossip service successfully initialized, let's start it...");
			gossipService.start();
		} catch (InterruptedException e) {
			System.err.println("Error while starting the gossip service: " + e.getMessage());
		} catch (UnknownHostException e) {
			System.err.println("Error while starting the gossip service: " + e.getMessage());
		}
	}
}
