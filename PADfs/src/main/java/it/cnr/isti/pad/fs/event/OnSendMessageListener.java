package it.cnr.isti.pad.fs.event;

import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

public interface OnSendMessageListener {
	void onRequestSendMessage(StorageMessage msg, String ip);
}
