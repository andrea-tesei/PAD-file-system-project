package it.cnr.isti.pad.fs.udpsocket;

import java.net.SocketAddress;

public interface IUDPSocket {
	
	abstract public boolean sendPacket(byte[] msg, SocketAddress remoteServerAddr);
	
	abstract public String receivePacket();
	
	public void closeConnection();
	
}
