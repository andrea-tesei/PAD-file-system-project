package it.cnr.isti.pad.UDPSocket;

import java.net.InetSocketAddress;

public class SocketRemoteInfo {

	private final int remotePort = 9099;
	private String remoteHost = "";
	private InetSocketAddress remoteServerAddr = null;
	
	public InetSocketAddress getRemoteServerAddr() {
		return remoteServerAddr;
	}

	public SocketRemoteInfo(String host) {
		this.remoteHost = host;
		this.remoteServerAddr = new InetSocketAddress(this.remoteHost, this.remotePort);
	}
	
	public String getRemoteHost() {
		return remoteHost;
	}


	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}


	public int getRemotePort() {
		return remotePort;
	}
}
