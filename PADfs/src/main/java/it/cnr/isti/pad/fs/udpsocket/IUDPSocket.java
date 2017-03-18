package it.cnr.isti.pad.fs.udpsocket;

import java.io.IOException;
import java.net.SocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * IUDPSocket interface. Interface for the UDP Socket connection.
 * 
 * @author Andrea Tesei
 *
 */
public interface IUDPSocket {
	
	abstract public boolean sendPacket(byte[] msg, SocketAddress remoteServerAddr);
	
	abstract public JSONObject receivePacket() throws JSONException, IOException;
	
	public void closeConnection();
	
}
