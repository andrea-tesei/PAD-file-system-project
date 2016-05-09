package it.cnr.isti.pad.fs.udpsocket;

public class Message {
	
	public static class TYPE {
		static int REQUEST = 0;
		static int RESPONSE = 1;
	}
	
	public static class COMMAND {
		static int GET = 2; // Retrieve single file
		static int PUT = 3; // Put single file
		static int LIST = 4; // List of files in destination node
//		static int UPDATE_LIST_FROM_CLIENT = 5; // Update the global list of files
		static int UPDATE = 5; // Update single file
		static int UPDATE_BACKUP = 6; // Update single backup file
		static int SEND_BACKUP = 7; // Send all backup copies to replica's node
	}

	public static class RETURNCODE {
		
	}
}
