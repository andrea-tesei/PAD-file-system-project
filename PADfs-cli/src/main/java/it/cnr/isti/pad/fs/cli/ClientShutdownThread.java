package it.cnr.isti.pad.fs.cli;

public class ClientShutdownThread implements Runnable {
	
	public ClientShutdownThread(){
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		StorageNodeCommandLineClient.keepRunning.set(false);
	}

}
