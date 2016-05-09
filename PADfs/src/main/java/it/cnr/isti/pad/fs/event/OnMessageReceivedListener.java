package it.cnr.isti.pad.fs.event;

import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

public interface OnMessageReceivedListener {
	StorageMessage onReceivedMessage();
}
