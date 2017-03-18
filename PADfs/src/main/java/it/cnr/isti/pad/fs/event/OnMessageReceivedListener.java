package it.cnr.isti.pad.fs.event;

import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

/**
 * Interface for New Message Event listener. 
 * 
 * @author Andrea Tesei
 *
 */
public interface OnMessageReceivedListener {
	/**
	 * Handler function for new StorageMessage arrived.
	 * @param msg the StorageMessage to be processed.
	 */
	void onReceivedMessage(StorageMessage msg);
}
