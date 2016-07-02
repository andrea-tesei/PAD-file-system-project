package it.cnr.isti.pad.fs.entry;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.pad.fs.api.ApiApp;
import it.cnr.isti.pad.fs.runnables.StorageShutdownThread;
import it.cnr.isti.pad.fs.storage.StorageNode;

public class App
{
	public static final Logger LOGGER = Logger.getLogger(App.class);

	public App(){ }
	
	public static void main( String[] args )
	{

		// Instanciation of Gossip Service
		Properties props = new Properties();
		try {
			props.load(App.class.getClassLoader().getResourceAsStream("log4j.properties"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.err.println("ERROR: An error occurs while opening log4j.properties file.");
		}
		org.apache.log4j.PropertyConfigurator.configure(props);

		File configFile = null;
		SpringApplication.run(ApiApp.class, args);
//		int virt = (int) (Math.log(5) / Math.log(2));
//		
//		ConsistentHasher<String, String> cHasherzzz = new ConsistentHasherImpl<>(
//				virt,
//				ConsistentHasher.getStringToBytesConverter(), 
//				ConsistentHasher.getStringToBytesConverter(), 
//				ConsistentHasher.SHA1);
//		
////		// TODO: add all ids and try here
//		String node1 = "e17959ba-387f-11e6-ac61-9e71128cae77";
//		String node2 = "e1795dde-387f-11e6-ac61-9e71128cae77";
//		String node3 = "e1795f8c-387f-11e6-ac61-9e71128cae77";
//		String node4 = "e17964dc-387f-11e6-ac61-9e71128cae77";
//		String node5 = "e1796676-387f-11e6-ac61-9e71128cae77";
//		
//		cHasherzzz.addBucket(node1);
//		cHasherzzz.addBucket(node2);
//		cHasherzzz.addBucket(node3);
//		cHasherzzz.addBucket(node4);
//		cHasherzzz.addBucket(node5);
//		
//		List<ByteBuffer> virtualBucketsNode1 = cHasherzzz.getAllVirtualBucketsFor(node1);
//		List<ByteBuffer> virtualBucketsNode4 = cHasherzzz.getAllVirtualBucketsFor(node4);
//		List<String> files = new ArrayList<>();
//		files.add("padfs.log");
//		
//		
//		
//		for(ByteBuffer virtualb : virtualBucketsNode4){
//			System.out.println("Descendant bucket is : " + cHasherzzz.getDescendantBucketKey(node4, virtualb));
//			System.out.println("Corresponding file to put in there is: " + ((cHasherzzz.getMembersForVirtualBucket(node4, virtualb, files).isEmpty()) ? "null" : cHasherzzz.getMembersForVirtualBucket(node4, virtualb, files)));
//		}
//		
//		
//		
//		
//		
//		System.out.println("");
//		
		
		// TODO: test new consistenthashing function + test PUT and calculate next and prev
		// getAllVirtualBucketsFor ok, getMemmbersForVirtualBucket ok, 
//		cHasherzzz.getAllVirtualBucketsFor(node1).forEach(b -> System.out.println(b));
//		String data = "Anna Frank bugiardona";
//		List<String> memb = new ArrayList<String>();
//		memb.add(data);
//		ArrayList<ByteBuffer> selectedKey = new ArrayList<>();
//		cHasherzzz.getAllBuckets().forEach(bucketName -> 
//		{
//			List<ByteBuffer> virtualNodes = cHasherzzz.getAllVirtualBucketsFor(bucketName);
//			virtualNodes.forEach(virtNode ->
//			{
//				if(!cHasherzzz.getMembersForVirtualBucket(bucketName, virtNode, memb).isEmpty()){
//					selectedKey.add(virtNode);
////					memb.add(bucketName);
//				}
//			});
//		});
//		selectedKey.forEach(key -> System.out.println("The virtual node code is: " + key));
//		memb.forEach(s -> System.out.println(s));
				
//		System.exit(0);
		if (args.length == 1) {
			configFile = new File("./" + args[0]);
			try {
				StorageNode node = new StorageNode(configFile);
				Runtime.getRuntime().addShutdownHook(new Thread(new StorageShutdownThread(node)));
				node.start();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//ArrayList<String> bucketFor = node.retrieveBucketForMember("prova.txt");
			//bucketFor.forEach(bucket -> System.out.println("BucketFor: " + bucket));
//			ExecutorService exec = Executors.newCachedThreadPool();

			//			sendert = new StorageSenderThreadImpl();
			//			receivert = null;
			//			try {
			//				 receivert = new StorageReceiverThreadImpl();
			//			} catch (UnknownHostException e) {
			//				// TODO Auto-generated catch block
			//				e.printStackTrace();
			//			} catch (SocketException e) {
			//				// TODO Auto-generated catch block
			//				e.printStackTrace();
			//			}
			//			exec.execute(receivert);
			//			JSONObject sendMsg = new JSONObject();
			//			try {
			//				File file = new File("./Coupon.pdf");
			//				sendMsg.put("host", receivert.getServerHost());
			//				sendMsg.put("type", "REQUEST");
			//				sendMsg.put("command", "GET");
			//				byte[] provaFileBA = Files.toByteArray(file);
			//				sendMsg.put("filename", file.getName());
			//				sendMsg.put("file",Base64.encodeBase64String(provaFileBA));
			//				StorageMessage sendm = new StorageMessage(sendMsg);



			// TODO: it works! But it sends message twice: check why.
			//  	 if you call triggerListeners the send functions is called by main thread.
			//		 if you call from executors, it will be processed in the future, (maybe) with a separate thread
			//		 Solutions: in this case listener is useless. Eliminate sendlistener and call thread with executor.
			//				sendert.addSendRequestToQueue(sendm, sendert.getClientsHosts().values().iterator().next().getRemoteServerAddr().getHostString());
			//				exec.execute(sendert);
			//				//App.triggerListeners(sendm, sendert.getClientsHosts().values().iterator().next().getRemoteServerAddr().getHostString());
			//			} catch (JSONException e) {
			// TODO Auto-generated catch block
			//				e.printStackTrace();
			//				App.LOGGER.error("The system encountered a problem while packeting your message. Please try again later.");
			//			} 
			//			try {
			//				App.udpServer = new UDPServer();
			//				App.clientsHandlerSocket = new UDPClientsHandler();
			//				UDPClientRunnable udpClientRunnable = new UDPClientRunnable();
			//				UDPServerRunnable udpServerRunnable = new UDPServerRunnable();
			//				Thread udpclientThread = new Thread(udpClientRunnable);
			//				Thread udpserverThread = new Thread(udpServerRunnable);
			//				udpclientThread.run();
			//				udpserverThread.run();
			//			} catch (UnknownHostException e) {
			//				e.printStackTrace();
			//				grs.getGossipService().get_gossipManager().shutdown();
			//				App.LOGGER.error("Error: a problem arise while extracting your address. Please check it and try again.");
			//			} catch (SocketException e) {
			//				e.printStackTrace();
			//				grs.getGossipService().get_gossipManager().shutdown();
			//				App.LOGGER.error("Error: a problem arise while setting up the server socket. Please try again later.");
			//			}
		} else {
			System.err.println("Error: settings file is missing. You must specify a configuration file.");
			return;
		}

	}

}
