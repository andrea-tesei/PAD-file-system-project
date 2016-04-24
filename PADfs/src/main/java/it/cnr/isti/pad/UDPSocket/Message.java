package it.cnr.isti.pad.UDPSocket;

public class Message {
	
	public static class TYPE {
		static int REQUEST = 0;
		static int RESPONSE = 1;
	}
	
	public static class COMMAND {
		static int GET = 2; // Retrieve single file
		static int PUT = 3; // Put single file
		static int LIST = 4; // List of files in File System
		static int UPDATE = 5; // Update single file
		static int SEND_BACKUP = 6; // Send backup copy to replica's node
	}

}
