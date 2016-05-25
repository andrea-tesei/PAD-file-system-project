package it.cnr.isti.pad.fs.udpsocket;

public class Message {
	
	public static class Type {
		public static final int REQUEST = 0;
		public static final int RESPONSE = 1;
	}
	
	public static class Command {
		public static final int GET = 2; // Retrieve single file
		public static final int PUT = 3; // Put single file
		public static final int LIST = 4; // List of files in destination node
		public static final int DELETE = 5; // Update single file
		public static final int PUT_BACKUP = 7; // Send all backup copies to replica's node
		public static final int UPDATE_BACKUP = 12; // Send single file update to replica's node
		public static final int DELETE_BACKUP = 13; // Delete single backup copy 
		public static final int ERASE_BACKUP = 15;
		public static final int ASK_FOR_BACKUP = 14; // Ask for fresh backup copy: used on Startup
		public static final int CONFLICT_RESOLUTION = 11;
	}

	public static class ReturnCode {
		public static final int OK = 8;
		public static final int ERROR = 9;
		public static final int NOT_EXISTS = 10;
	}
	
}
