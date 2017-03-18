package it.cnr.isti.pad.fs.entry;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.pad.fs.api.ApiApp;
import it.cnr.isti.pad.fs.runnables.StorageShutdownThread;
import it.cnr.isti.pad.fs.storage.StorageNode;

public class App
{
	public static final Logger LOGGER = Logger.getLogger(App.class);

	public App(){ }
	
	/**
	 * Main function of the whole project. This instantiate the Gossip Service, runs the API controller
	 * and finally sets up the Storage Node for this node.
	 * @param args contains the path of the file for the configuration
	 */
	public static void main( String[] args )
	{

		// Instanciation of Gossip Service
		Properties props = new Properties();
		try {
			props.load(App.class.getClassLoader().getResourceAsStream("log4j.properties"));
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.println("ERROR: An error occurs while opening log4j.properties file.");
		}
		org.apache.log4j.PropertyConfigurator.configure(props);

		File configFile = null;
		SpringApplication.run(ApiApp.class, args);
		if (args.length == 1) {
			configFile = new File("./" + args[0]);
			try {
				StorageNode node = new StorageNode(configFile);
				Runtime.getRuntime().addShutdownHook(new Thread(new StorageShutdownThread(node)));
				node.start();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (SocketException e) {
				e.printStackTrace();
			}
		} else {
			System.err.println("Error: settings file is missing. You must specify a configuration file.");
			return;
		}

	}

}
