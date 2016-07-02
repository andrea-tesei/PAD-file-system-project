package it.cnr.isti.pad.fs.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.code.gossip.LocalGossipMember;
import com.google.common.io.Files;

import it.cnr.isti.pad.fs.storage.Data;
import it.cnr.isti.pad.fs.storage.StorageNode;
import it.cnr.isti.pad.fs.udpsocket.Message;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;
import it.cnr.isti.pad.fs.udpsocket.Message.ReturnCode;
import voldemort.versioning.VectorClock;

@RestController
@EnableAutoConfiguration
@RequestMapping("/API/PADfs/")
public class ApiControllers {

	public static final Logger LOGGER = Logger.getLogger(ApiControllers.class);

	public ApiControllers(){

	}

	@RequestMapping("/hello/{name}")
	String hello(@PathVariable String name) {
		return "Hello, " + name + "!";
	}

	@RequestMapping("/GET")
	String getFile(@RequestParam(value = "filename") String fileName) {
		ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileName);
		StorageMessage response = null;
		if(bucketFor.size() > 1){
			StorageNode.LOGGER.warn("More than one bucket for " + fileName);
		} else if(!bucketFor.isEmpty() && !StorageNode.myId.equals(bucketFor.get(0))){
			// This file is stored on a remote node
			String ipBucketForFile = StorageNode.getIPFromID(bucketFor.get(0));
			int idRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage getRemoteFile = new StorageMessage(StorageNode.myHost,
					ipBucketForFile,
					Message.Type.REQUEST,
					idRequest,
					Message.Command.GET,
					-1,
					null,
					fileName,
					null,
					null);
			StorageNode.pendingRequest.put(idRequest, getRemoteFile);
			StorageNode.addRequestToQueue(getRemoteFile, ipBucketForFile);
			StorageNode.executeSenderThread();
			int attempts = 0; 
			while(StorageNode.pendingRequest.containsKey(idRequest) && attempts < 20){
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
				}
				attempts++;
			}
			StorageMessage responseForGetRequest = StorageNode.pendingResponse.remove(idRequest);
			if(responseForGetRequest != null){
				response = responseForGetRequest;
			} else
				response = new StorageMessage(StorageNode.myHost,
						ipBucketForFile,
						Message.Type.RESPONSE,
						idRequest,
						Message.Command.GET,
						ReturnCode.ERROR,
						new JSONArray().put(new JSONObject().put("errmsg", "The remote host did not receive the GET request within 5 seconds. Please retry.")),
						fileName,
						null,
						null); 

		} else {
			// This file is already stored here
			if(!StorageNode.myFiles.containsKey(fileName))
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.GET, 
						Message.ReturnCode.NOT_EXISTS, 
						null, 
						fileName, 
						null, 
						null);
			else {
				// Check if some conflict is present in the requested file
				if(!StorageNode.myFiles.get(fileName).hasConflict()){
					Data dataForGet = StorageNode.myFiles.get(fileName);
					response = new StorageMessage(StorageNode.myHost,
							"",
							Message.Type.RESPONSE, 
							-1, 
							Message.Command.GET, 
							Message.ReturnCode.OK, 
							null, 
							dataForGet.getFileName(), 
							dataForGet, 
							null);
				} else {
					Data dataToSolve = StorageNode.myFiles.get(fileName);
					JSONArray output = new JSONArray();
					dataToSolve.getConflicts().forEach(conflictdata -> output.put(conflictdata.getVersion().getTimestamp()));
					response = new StorageMessage(StorageNode.myHost,
							"",
							Message.Type.RESPONSE, 
							-1, 
							Message.Command.GET, 
							Message.ReturnCode.CONFLICTS_EXISTS, 
							output, 
							dataToSolve.getFileName(), 
							null, 
							null);
				}
			}
		}
		return convertJsonFormat(response.toJSONObject()).toString();

	}

	@RequestMapping("/ConflictResolution")
	String ConflictResolution(@RequestParam(value = "filename") String fileName, @RequestParam(value = "tschosen") @PathVariable Long tsChoosen) {
		ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileName);
		StorageMessage response = null;
		if(bucketFor.size() > 1){
			StorageNode.LOGGER.warn("More than one bucket for " + fileName);
		} else if(!bucketFor.isEmpty() && !StorageNode.myId.equals(bucketFor.get(0))){
			// This file isn't stored in this node. Ask to remote one
			String ipRemoteNode = StorageNode.getIPFromID(bucketFor.get(0));
			int idRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage remoteConflictResolutionMSG = new StorageMessage(StorageNode.myHost,
																		ipRemoteNode,
																		Message.Type.REQUEST, 
																		idRequest, 
																		Message.Command.CONFLICT_RESOLUTION, 
																		-1, 
																		new JSONArray().put(tsChoosen), 
																		fileName, 
																		null, 
																		null);
			StorageNode.pendingRequest.put(idRequest, remoteConflictResolutionMSG);
			StorageNode.addRequestToQueue(remoteConflictResolutionMSG, ipRemoteNode);
			StorageNode.executeSenderThread();
			int attempts = 0; 
			while(StorageNode.pendingRequest.containsKey(idRequest) && attempts < 20){
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
				}
				attempts++;
			}
			StorageMessage responseForGetRequest = StorageNode.pendingResponse.remove(idRequest);
			if(responseForGetRequest != null){
				response = responseForGetRequest;
			} else
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE,
						idRequest,
						Message.Command.CONFLICT_RESOLUTION,
						ReturnCode.ERROR,
						new JSONArray().put(new JSONObject().put("errmsg", "The remote host did not receive the CONFLICT_RESOLUTION request within 5 seconds. Please retry.")),
						fileName,
						null,
						null); 
		} else {
			// This file could be stored in this node.
			if(!StorageNode.myFiles.containsKey(fileName))
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.CONFLICT_RESOLUTION, 
						Message.ReturnCode.NOT_EXISTS, 
						null, 
						fileName, 
						null, 
						null);
			else {
				// Check if some conflict is present in the requested file
				int idRequestForUpdateBackup = StorageNode.resolveConflictResolution(tsChoosen, fileName);		
				StorageNode.executeSenderThread();
				StorageNode.executeResponseHandlerThread();
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.CONFLICT_RESOLUTION, 
						Message.ReturnCode.ERROR, 
						null, 
						fileName, 
						null, 
						null);
			}
		}
		return convertJsonFormat(response.toJSONObject()).toString();
	}

	@RequestMapping(value = "/PUT", method = RequestMethod.POST)
	String putFile(@RequestParam(value = "filename") String fileName, @RequestParam(value = "file") String fileBase64) {
		ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileName);
		StorageMessage response = null;
		if(bucketFor.size() > 1){
			StorageNode.LOGGER.warn("More than one bucket for " + fileName);
		} else if(!bucketFor.isEmpty() && !StorageNode.myId.equals(bucketFor.get(0))){
			// This file belongs to a remote node
			Data newData = new Data(-1, false, "root", bucketFor.get(0), fileName, "./files/", fileBase64, null);
			// Forward request to right bucket
			String ipRightBucket = StorageNode.getIPFromID(bucketFor.get(0));
			int currentIdRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage putRemoteFile = new StorageMessage(StorageNode.myHost,
					ipRightBucket,
					Message.Type.REQUEST,
					currentIdRequest,
					Message.Command.PUT,
					-1,
					null,
					fileName,
					newData,
					null);
			StorageNode.pendingRequest.put(currentIdRequest, putRemoteFile);
			StorageNode.addRequestToQueue(putRemoteFile, ipRightBucket);
			StorageNode.executeSenderThread();
			int attempts = 0;
			while(StorageNode.pendingRequest.containsKey(currentIdRequest) && attempts < 20){
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
				}
				attempts++;
			}
			StorageMessage responseForPut = StorageNode.pendingResponse.remove(currentIdRequest);
			if(responseForPut != null){
				response = responseForPut;
			} else
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.PUT, 
						Message.ReturnCode.ERROR, 
						new JSONArray().put(new JSONObject().put("errmsg", "The remote host did not receive the PUT request within 5 secs. Please retry.")), 
						fileName, 
						null, 
						null);
		} else {
			// This file has to be stored in this node
			try {
				Data newData = new Data(-1, false, "root", StorageNode.myId, fileName, "./files/", fileBase64, null);
				int idRequestForUpdateBackup =  StorageNode.putData(newData);
				JSONArray output = new JSONArray();
				if(idRequestForUpdateBackup == -1)
					output.put(new JSONObject().put("error", "An error occurred while storing the given file."));
				else
					output.put(new JSONObject().put("status", "ok"));
				StorageNode.executeSenderThread();
				StorageNode.executeResponseHandlerThread();
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.PUT, 
						Message.ReturnCode.ERROR, 
						output, 
						fileName, 
						null, 
						null);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				ApiControllers.LOGGER.error("A problem arise when storing file " + fileName + " in this node.");
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE, 
						-1, 
						Message.Command.PUT, 
						Message.ReturnCode.ERROR, 
						null, 
						fileName, 
						null, 
						null);
			}
		}
		return convertJsonFormat(response.toJSONObject()).toString();
	}

	@RequestMapping("/LIST")
	String listFiles() {
		ArrayList<Integer> idsListRequest = new ArrayList<Integer>();
		for(LocalGossipMember node : StorageNode.grs.getGossipService().get_gossipManager().getMemberList()){
			int currListIdRequest = StorageNode.requestIDCounter.getAndIncrement();
			idsListRequest.add(currListIdRequest);
			StorageMessage askForFileList = new StorageMessage(StorageNode.myHost,
					node.getHost(),
					Message.Type.REQUEST,
					currListIdRequest,
					Message.Command.LIST,
					-1,
					null,
					null,
					null,
					null);
			StorageNode.pendingRequest.put(currListIdRequest, askForFileList);
			StorageNode.addRequestToQueue(askForFileList, node.getHost());
		}
		StorageNode.executeSenderThread();
		int attempts = 0;
		while(StorageNode.pendingRequest.size() > 0 && attempts < 20){
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
			}
			attempts++;
		}
		ArrayList<String> fileList = new ArrayList<String>();
		StorageNode.myFiles.forEach((name, file) -> fileList.add(name));
		// Reading incoming messages for list files
		idsListRequest.forEach(idRequest -> 
		{
			StorageMessage listResponse = StorageNode.pendingResponse.remove(idRequest);
			if(listResponse != null){
				if(listResponse.getReturnCode() != Message.ReturnCode.ERROR){
					JSONArray receivedListFiles = listResponse.getOutput();
					if(!receivedListFiles.getJSONObject(0).has("status")){
						for(int j = 0; j < receivedListFiles.length(); j++){
							Data newDataFromRemoteNode = null;
							try {
								newDataFromRemoteNode = new Data(receivedListFiles.getJSONObject(j));
								fileList.add(newDataFromRemoteNode.getFileName());
							} catch (Exception e) {
								fileList.add("ERROR");
								ApiControllers.LOGGER.error("Error while parsing LIST response. Please try again. Error = " + e.getStackTrace());
							}
						}
					}
				} else {
					fileList.add("ERROR:" + listResponse.getHost());
					ApiControllers.LOGGER.warn("The list file command encountered a problem while receiving file's list from host = " + listResponse.getHost());
				}
			} else {
				fileList.add("ERROR");
			}
		});
		StorageMessage response = null;
		if(fileList.contains("ERROR"))
			response = new StorageMessage(StorageNode.myHost,
					"",
					Message.Type.RESPONSE,
					-1,
					Message.Command.LIST,
					Message.ReturnCode.ERROR,
					null,
					null,
					null,
					null);
		else {
			fileList.sort(new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					return s1.compareToIgnoreCase(s2);
				}
			});
			response = new StorageMessage(StorageNode.myHost,
					"",
					Message.Type.RESPONSE,
					-1,
					Message.Command.LIST,
					Message.ReturnCode.OK,
					new JSONArray().put(fileList),
					null,
					null,
					null);
		}
		return convertJsonFormat(response.toJSONObject()).toString();
	}

	@RequestMapping("/DELETE")
	String delFile(@RequestParam(value = "filename") String fileNameForDelete) {
		ArrayList<String> bucketFor = StorageNode.retrieveBucketForMember(fileNameForDelete);
		StorageMessage response = null;
		if(bucketFor.size() > 1){
			ApiControllers.LOGGER.warn("More than one bucket for " + fileNameForDelete);
		} else if(!bucketFor.isEmpty() && !StorageNode.myId.equals(bucketFor.get(0))){
			// Delete file on remote node if exists
			String remoteip = StorageNode.getIPFromID(bucketFor.get(0));
			int currDeleteIdRequest = StorageNode.requestIDCounter.getAndIncrement();
			StorageMessage remoteDelMessage = new StorageMessage(StorageNode.myHost,
					remoteip,
					Message.Type.REQUEST,
					currDeleteIdRequest,
					Message.Command.DELETE,
					-1,
					null,
					fileNameForDelete,
					null,
					null);
			StorageNode.pendingRequest.put(currDeleteIdRequest, remoteDelMessage);
			StorageNode.addRequestToQueue(remoteDelMessage, remoteip);
			StorageNode.executeSenderThread();
			// Wait for response
			int attempts = 0;
			while(StorageNode.pendingRequest.containsKey(currDeleteIdRequest) && attempts < 20){
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
				}
				attempts++;

			}
			StorageMessage remoteDeleteResponse = StorageNode.pendingResponse.remove(currDeleteIdRequest);
			if(remoteDeleteResponse != null){
				response = remoteDeleteResponse;
			} else
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE,
						-1,
						Message.Command.DELETE,
						Message.ReturnCode.ERROR,
						new JSONArray().put(new JSONObject().put("errmsg", "The remote host did not receive the DELETE request within 5 seconds. Please retry.")),
						fileNameForDelete,
						null,
						null); 
		} else {
			// Delete file on this node if exists
			try{
				int idRequestDeleteBackup = StorageNode.deleteData(fileNameForDelete);
				if(idRequestDeleteBackup != -1){
					StorageNode.addResponseToHandlerQueue(idRequestDeleteBackup);
					StorageNode.executeResponseHandlerThread();
					response = new StorageMessage(StorageNode.myHost,
							"",
							Message.Type.RESPONSE,
							-1,
							Message.Command.DELETE,
							Message.ReturnCode.OK,
							null,
							fileNameForDelete,
							null,
							null);
				} else
					response = new StorageMessage(StorageNode.myHost,
							"",
							Message.Type.RESPONSE,
							-1,
							Message.Command.DELETE,
							Message.ReturnCode.NOT_EXISTS,
							null,
							fileNameForDelete,
							null,
							null);
			} catch (IOException e) {
				response = new StorageMessage(StorageNode.myHost,
						"",
						Message.Type.RESPONSE,
						-1,
						Message.Command.DELETE,
						Message.ReturnCode.ERROR,
						new JSONArray().put(new JSONObject().put("errmsg", "An error occurred while deleting the given file. Error = " + e.getStackTrace())),
						fileNameForDelete,
						null,
						null);
			}
		}
		return convertJsonFormat(response.toJSONObject()).toString();
	}
	
	static JsonNode convertJsonFormat(JSONObject json) {
	    ObjectNode ret = JsonNodeFactory.instance.objectNode();

	    @SuppressWarnings("unchecked")
	    Iterator<String> iterator = json.keys();
	    for (; iterator.hasNext();) {
	        String key = iterator.next();
	        Object value;
	        try {
	            value = json.get(key);
	        } catch (JSONException e) {
	            throw new RuntimeException(e);
	        }
	        if (json.isNull(key))
	            ret.putNull(key);
	        else if (value instanceof String)
	            ret.put(key, (String) value);
	        else if (value instanceof Integer)
	            ret.put(key, (Integer) value);
	        else if (value instanceof Long)
	            ret.put(key, (Long) value);
	        else if (value instanceof Double)
	            ret.put(key, (Double) value);
	        else if (value instanceof Boolean)
	            ret.put(key, (Boolean) value);
	        else if (value instanceof JSONObject)
	            ret.put(key, convertJsonFormat((JSONObject) value));
	        else if (value instanceof JSONArray)
	            ret.put(key, convertJsonFormat((JSONArray) value));
	        else
	            throw new RuntimeException("not prepared for converting instance of class " + value.getClass());
	    }
	    return ret;
	}
	
	static JsonNode convertJsonFormat(JSONArray json) {
	    ArrayNode ret = JsonNodeFactory.instance.arrayNode();
	    for (int i = 0; i < json.length(); i++) {
	        Object value;
	        try {
	            value = json.get(i);
	        } catch (JSONException e) {
	            throw new RuntimeException(e);
	        }
	        if (json.isNull(i))
	            ret.addNull();
	        else if (value instanceof String)
	            ret.add((String) value);
	        else if (value instanceof Integer)
	            ret.add((Integer) value);
	        else if (value instanceof Long)
	            ret.add((Long) value);
	        else if (value instanceof Double)
	            ret.add((Double) value);
	        else if (value instanceof Boolean)
	            ret.add((Boolean) value);
	        else if (value instanceof JSONObject)
	            ret.add(convertJsonFormat((JSONObject) value));
	        else if (value instanceof JSONArray)
	            ret.add(convertJsonFormat((JSONArray) value));
	        else
	            throw new RuntimeException("not prepared for converting instance of class " + value.getClass());
	    }
	    return ret;
	}
}
