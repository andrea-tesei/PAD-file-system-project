package it.cnr.isti.pad.fs.udpsocket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import it.cnr.isti.pad.fs.storage.StorageNode;

public class UDPClientsHandler implements IUDPSocket {
	
	private DatagramSocket dgsocket = null;
	private HashMap<String, SocketRemoteInfo> nodes = new HashMap<String, SocketRemoteInfo>();
	public static final Logger LOGGER = Logger.getLogger(UDPClientsHandler.class);

	public UDPClientsHandler() throws SocketException{
		this.dgsocket = new DatagramSocket();
		if(StorageNode.grs != null){
			StorageNode.grs.getGossipService().get_gossipManager().getMemberList().forEach(node -> 
							{
								if(!node.getHost().equals(StorageNode.grs.getGossipService().get_gossipManager().getMyself().getHost())){
									UDPClientsHandler.LOGGER.info("Retrieving client socket info for: " + node.getHost() + ":" + node.getPort());
									SocketRemoteInfo info = new SocketRemoteInfo(node.getHost());
									nodes.put(node.getHost(), info);
								}
							});
		}
	}
	
	public SocketAddress getSocketAddressFromIP(String ip){
		ArrayList<SocketAddress> result = new ArrayList<SocketAddress>();
		this.nodes.forEach((host,info) -> 
		{ 
			if(host.equals(ip))
				result.add(info.getRemoteServerAddr());
		});
		return result.size() == 0 ? null : result.get(0);
	}
	
	@Override
	public boolean sendPacket(byte[] msg, SocketAddress remoteServerAddr) {
		try {
			// Transforming length of the packet in byte-encoding
			byte[] length_bytes = new byte[4];
			length_bytes[0] = (byte) (msg.length >> 24);
			length_bytes[1] = (byte) ((msg.length << 8) >> 24);
			length_bytes[2] = (byte) ((msg.length << 16) >> 24);
			length_bytes[3] = (byte) ((msg.length << 24) >> 24);

			ByteBuffer byteBuffer = ByteBuffer.allocate(4 + msg.length);
			byteBuffer.put(length_bytes);
			byteBuffer.put(msg);
			byte[] buf = byteBuffer.array();
			dgsocket.send(new DatagramPacket(buf, buf.length, remoteServerAddr));
		} catch (IOException e) {
			e.printStackTrace();
			UDPClientsHandler.LOGGER.error("An error occurred while sending packet: " + e.getMessage());
			return false;
		}
		return true;
	}
	
	public HashMap<String, SocketRemoteInfo> getNodes() {
		return nodes;
	}

	@Override
	public JSONObject receivePacket() throws JSONException {
		String receivedMessage = "";
		DatagramPacket p = null;
		try {
			byte[] buf = new byte[this.dgsocket.getReceiveBufferSize()];
			p = new DatagramPacket(buf, buf.length);
			this.dgsocket.receive(p);

			// Retrieving packet length
			int packet_length = 0;
			for (int i = 0; i < 4; i++) {
				int shift = (4 - 1 - i) * 8;
				packet_length += (buf[i] & 0x000000FF) << shift;
			}

			// Read content of the message
			byte[] json_bytes = new byte[packet_length];
			for (int i = 0; i < packet_length; i++) {
				json_bytes[i] = buf[i + 4];
			}
			receivedMessage = new String(json_bytes);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("An error occurred while receiving packet from: " + p.getAddress().getHostAddress() + " : " + e.getMessage());
			return null;
		}
		return new JSONObject(receivedMessage);
	}
	
	@Override
	public void closeConnection(){
		this.dgsocket.close();
		this.dgsocket = null;
	}
}
