package it.cnr.isti.pad.fs.runnables;

import java.io.IOException;
import org.apache.log4j.Logger;
import it.cnr.isti.pad.fs.storage.StorageNode;

public class StorageShutdownThread implements Runnable {

	private StorageNode nodeToShutdown;
	
	public static final Logger LOGGER = Logger.getLogger(StorageShutdownThread.class);
	
	public StorageShutdownThread(StorageNode node){
		this.nodeToShutdown = node;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			this.nodeToShutdown.shutdown();
			StorageShutdownThread.LOGGER.info("The node is shutting down...");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			StorageShutdownThread.LOGGER.error("An error occurred while saving existing files.");
		}
	}

}
