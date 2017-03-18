package it.cnr.isti.pad.fs.udpsocket;

import java.net.InetSocketAddress;

/**
 * SocketRemoteInfo class. It contains all the informations for a Socket remote client.
 * 
 * @author Andrea Tesei
 *
 */
public class SocketRemoteInfo {

	private final int remotePort = 9099;
	private String remoteHost = "";
	private InetSocketAddress remoteServerAddr = null;
	

	/**
	 * SocketRemoteInfo constructor.
	 * @param host the host of the remote client.
	 */
	public SocketRemoteInfo(String host) {
		this.remoteHost = host;
		this.remoteServerAddr = new InetSocketAddress(this.remoteHost, this.remotePort);
	}

	/**
	 * Function getRemoteServerAddr. Retrieve the remote InetSocketAddress of the remote client.
	 * @return InetSocketAddress of this SocketRemote Informations.
	 */
	public InetSocketAddress getRemoteServerAddr() {
		return remoteServerAddr;
	}
	
	/**
	 * Function getRemoteHost. Retrieve the host of the remote client.
	 * @return the string representing the ip of the remote client.
	 */
	public String getRemoteHost() {
		return remoteHost;
	}

	/**
	 * Function setRemoteHost. Set the host of the remote client.
	 * @param remoteHost the host to be set in this SocketRemoteInfo.
	 */
	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}


	/**
	 * Function getRemotePort. Retrieve the remote port of the remote client.
	 * @return the port of the remote client socket.
	 */
	public int getRemotePort() {
		return remotePort;
	}
}
