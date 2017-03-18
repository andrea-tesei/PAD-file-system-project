package it.cnr.isti.pad.fs.event;

import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

/**
 * Interface for Send Message Event.
 * 
 * @author Andrea Tesei
 *
 */
public interface OnSendMessageListener {
	/**
	 * Handler function for the send message operation.
	 * @param msg the message to be send.
	 * @param ip the ip of the destination node.
	 */
	void onRequestSendMessage(StorageMessage msg, String ip);
}
