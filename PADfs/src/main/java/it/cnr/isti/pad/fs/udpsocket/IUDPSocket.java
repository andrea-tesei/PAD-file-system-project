package it.cnr.isti.pad.fs.udpsocket;

import java.net.SocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

public interface IUDPSocket {
	
	abstract public boolean sendPacket(byte[] msg, SocketAddress remoteServerAddr);
	
	abstract public JSONObject receivePacket() throws JSONException;
	
	public void closeConnection();
	
}
