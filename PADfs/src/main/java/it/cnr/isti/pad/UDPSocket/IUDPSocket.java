package it.cnr.isti.pad.UDPSocket;

import java.net.SocketAddress;

public interface IUDPSocket {
	
	public boolean sendPacket(byte[] msg, SocketAddress remoteServerAddr);
	
	public String receivePacket();
	
	public boolean closeConnection();
	
}
