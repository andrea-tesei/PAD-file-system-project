package it.cnr.isti.pad.fs.cli;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.codec.binary.Base64;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.cnr.isti.pad.fs.datatypes.Message;
import it.cnr.isti.pad.fs.datatypes.StorageMessage;

@Component
public class StorageNodeCommandLineClient implements CommandLineRunner  {

	public static AtomicBoolean keepRunning = new AtomicBoolean(true);

	@Override
	public void run(String... arg0) throws Exception {
		if (!(arg0.length < 1)) {
			File configFile = new File(arg0[0]);
			if(configFile.exists()){
				ArrayList<String> listKnownHosts = new ArrayList<String>();
				try {
					Scanner reader = new Scanner(System.in);
					Files.lines(configFile.toPath()).forEach(line -> listKnownHosts.add(line));
					Runtime.getRuntime().addShutdownHook(new Thread(new ClientShutdownThread()));
					RestTemplate restTemplate = new RestTemplate();
					List<HttpMessageConverter<?>> currentMsgConverter = restTemplate.getMessageConverters();
					HttpMessageConverter formHttpMessageConverter = new FormHttpMessageConverter();
					HttpMessageConverter stringHttpMessageConverternew = new StringHttpMessageConverter();
					currentMsgConverter.add(stringHttpMessageConverternew);
					currentMsgConverter.add(formHttpMessageConverter);
					restTemplate.setMessageConverters(currentMsgConverter);
					System.out.println("Welcome back!");
					System.out.println("This is your distributed file system command-line:");
					while(keepRunning.get()){
						System.out.println("Type one of these commands: PUT:[filename], GET:[filename], LIST, DELETE:[filename], CONFLICT_RESOLUTION:[filename]:[chosenTimestamp], QUIT or HELP to print additional explanation.");
						String input = reader.nextLine();
						List<String> commandAndArgs = null;
						if(input.contains(":"))
							commandAndArgs = Arrays.asList(input.split(":"));
						else {
							commandAndArgs = new ArrayList<String>();
							commandAndArgs.add(input);
						}
						switch(commandAndArgs.get(0)){
						case "GET":
							if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
								String fileName = commandAndArgs.get(1);
								StorageMessage getResponse = null;
								for(String ip :listKnownHosts){
									try{
										getResponse = restTemplate.getForObject("http://" + ip + ":8090/API/PADfs/GET?filename=" + fileName, StorageMessage.class);
										if(getResponse.getRc() == Message.ReturnCode.ERROR) {
											if(getResponse.getOutput().get(0).has("error")){
												if(getResponse.getOutput().get(0).get("error").equals("The remote host did not receive the request within 10 seconds. Please retry."))
													System.out.println("The remote node did not received the request. Trying another node...\n");
												else {
													System.err.println("The remote node " + getResponse.getHost() + " during GET operation for " + fileName + ". Error = " + getResponse.getOutput().get(0).get("error"));
													break;
												}
											}
										} else if(getResponse.getRc() == Message.ReturnCode.NOT_EXISTS){
											System.out.println("The requested file does not exist in the file system.\n");
											break;
										} else if(getResponse.getRc() == Message.ReturnCode.OK){
											// Save file locally
											byte[] rcvdFile = Base64.decodeBase64(getResponse.getData().getFile());
											// save the file to disk
											File filesDir = new File("./download");
											try {
												if(!filesDir.exists()){
													filesDir.mkdir();
												}
												File fileToSave = new File("./download/" + getResponse.getData().getFileName());
												Files.write(fileToSave.toPath(), rcvdFile);
												System.out.println("The requested file has been received and saved in: " + "./download/"+ getResponse.getData().getFileName() + "\n");
											} catch (IOException e) {
												System.err.println("An error occurs while saving file in " + "./download/" + getResponse.getData().getFileName() + "\n");
											}
											break;
										} else if(getResponse.getRc() == Message.ReturnCode.CONFLICTS_EXISTS){
											// Print all received timestamps and let user choose one of these
											System.out.println("The requested file has some conflict that must be resolved. Look at next versions list and choose the right one:");
											getResponse.getOutput().forEach(json -> System.out.println(fileName + ":" + json));
											System.out.println("Once you have chosen your version, type the CONFLICT_RESOLUTION message with the name of the file and the selected version in order to perform this operation.\n");
											break;
										}
									} catch (RestClientException e){
										System.out.println("The node " + ip + " is down. Update your node's list file.");
									}
								}
							} else 
								System.err.println("Error in GET: please specify the name of the file you want to download.\n");
							break;
						case "PUT":
							if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
								String fileName = commandAndArgs.get(1);
								File newFile = new File("./"+fileName);
								if(newFile.exists()){
									for(String ip :listKnownHosts){
										try{
											byte[] fileBA = Files.readAllBytes(newFile.toPath());
											MultiValueMap<String, String> mapParams = new LinkedMultiValueMap<String, String>();
											mapParams.add("filename", fileName);
											mapParams.add("file", Base64.encodeBase64String(fileBA));
											String postResponseForPut = restTemplate.postForObject("http://" + ip +":8090/API/PADfs/PUT", mapParams, String.class);
											System.out.println("Arrives: " + postResponseForPut);
											ObjectMapper mapper = new ObjectMapper();
											JsonNode actualObj = mapper.readTree(postResponseForPut);
											System.out.println("Parsed: " + actualObj);
											StorageMessage getResponseForPut = new StorageMessage(actualObj);
											if(getResponseForPut.getRc() == Message.ReturnCode.ERROR){
												if(getResponseForPut.getOutput().get(0).has("error")){
													if(getResponseForPut.getOutput().get(0).get("error").equals("The remote host did not receive the request within 10 seconds. Please retry."))
														System.out.println("The remote node did not received the request. Trying another node...\n");
													else {
														System.err.println("The remote node " + getResponseForPut.getHost() + " during PUT operation for " + fileName + ". Error = " + getResponseForPut.getOutput().get(0).get("error"));
														break;
													}
												}
											} else {
												System.out.println("The remote node " + getResponseForPut.getHost() + " has stored the file " + fileName + " successfully. Message: " + getResponseForPut.getOutput() + ".\n");
												break;
											}
										} catch (RestClientException e){
											System.out.println("The node " + ip + " is down. Update your node's list file.");
										}
									}
								} else {
									System.err.println("The given file is not present in the local directory.\n");
								}
							} else
								System.err.println("Error in PUT: please specify the name of the file you want to store.\n");
							break;
						case "LIST":
							for(String ip :listKnownHosts){
								try{
									StorageMessage getResponseForList = restTemplate.getForObject("http://" + ip +":8090/API/PADfs/LIST", StorageMessage.class);
									if(getResponseForList.getRc() == Message.ReturnCode.ERROR){
										if(getResponseForList.getOutput().get(0).has("error")){
											if(getResponseForList.getOutput().get(0).get("error").equals("The remote host did not receive the request within 10 seconds. Please retry."))
												System.out.println("The remote node did not received the request. Trying another node...\n");
											else {
												System.err.println("The remote node " + getResponseForList.getHost() + " during LIST operation. Error = " + getResponseForList.getOutput().get(0).get("error"));
												break;
											}
										}

									} else {
										System.out.println("List files:");
										getResponseForList.getOutput().forEach(json -> System.out.println(json));
										System.out.println("");
										break;
									}
								} catch (RestClientException e){
									System.out.println("The node " + ip + " is down. Update your node's list file.");
								}
							}
							break;
						case "DELETE":
							if(commandAndArgs.size() > 1 && !commandAndArgs.get(1).equals("")){
								String fileName = commandAndArgs.get(1);
								for(String ip :listKnownHosts){
									try{
										StorageMessage getResponseForDelete = restTemplate.getForObject("http://" + ip + ":8090/API/PADfs/DELETE?filename=" + fileName, StorageMessage.class);
										if(getResponseForDelete.getRc() == Message.ReturnCode.ERROR){
											if(getResponseForDelete.getOutput().get(0).has("error")){
												if(getResponseForDelete.getOutput().get(0).get("error").equals("The remote host did not receive the request within 10 seconds. Please retry."))
													System.out.println("The remote node did not received the request. Trying another node...\n");
												else {
													System.err.println("The remote node " + getResponseForDelete.getHost() + " during DELETE operation for " + fileName + ". Error = " + getResponseForDelete.getOutput().get(0).get("error"));
													break;
												}
											}
										} else if(getResponseForDelete.getRc() == Message.ReturnCode.NOT_EXISTS){
											System.out.println("The requested file does not exist in the file system.\n");
											break;
										} else {
											System.out.println("The remote node has completed DELETE operation of " + fileName + "\n");
											break;
										}
									} catch (RestClientException e){
										System.out.println("The node " + ip + " is down. Update your node's list file.");
									}
								}
							} else 
								System.err.println("Error in DELETE: please specify the name of the file you want to delete." + "\n");
							break;
						case "CONFLICT_RESOLUTION":
							if(commandAndArgs.size() > 2 && !commandAndArgs.get(2).equals("")){
								Long tsChosen = Long.parseLong(commandAndArgs.get(2));
								String fileName = commandAndArgs.get(1);
								for(String ip :listKnownHosts){
									try{
										StorageMessage getResponseForConflictResolution = restTemplate.getForObject("http://" + ip +":8090/API/PADfs/ConflictResolution?filename=" + fileName + "&tschosen=" + tsChosen, StorageMessage.class);
										if(getResponseForConflictResolution.getRc() == Message.ReturnCode.ERROR){
											if(getResponseForConflictResolution.getOutput().get(0).has("error")){
												if(getResponseForConflictResolution.getOutput().get(0).get("error").equals("The remote host did not receive the request within 10 seconds. Please retry."))
													System.out.println("The remote node did not received the request. Trying another node...\n");
												else {
													System.err.println("The remote node " + getResponseForConflictResolution.getHost() + " during CONFLICT_RESOLUTION operation for " + fileName + ". Error = " + getResponseForConflictResolution.getOutput().get(0).get("error"));
												}
											}
										} else if(getResponseForConflictResolution.getRc() == Message.ReturnCode.NOT_EXISTS){
											System.out.println("The given filename is wrong: the file " + fileName + " does not exists in this file system.\n");
											break;
										} else {
											// Ok message
											System.out.print("The file " + fileName + " has been updated to the selected version.\n");
											break;
										}
									} catch (RestClientException e){
										System.out.println("The node " + ip + " is down. Update your node's list file.");
									}
								}
							} else
								System.err.println("Error in CONFLICT_RESOLUTION: please specify the timestamp chosen.\n");
							break;
						case "HELP":
							System.out.println("This is the help message of PADfs-cli.\n\n"
									+ "The active features of this version provides the following methods:\n\n"
									+ "\t-GET: return the content of the requested file (syntax: GET:filename);\n\n"
									+ "\t-PUT: store the given file in the file system (syntax: PUT:filename);\n\n"
									+ "\t-LIST: returns the list of the file in the file system;\n\n"
									+ "\t-DELETE: delete the given file, if exists (syntax: DELETE:filename);\n\n"
									+ "\t-CONFLICT_RESOLUTION: manage conflict for the given file (syntax: CONFLICT_RESOLUTION:timestamp:filename);\n\n"
									+ "\t-HELP: to print this help;\n\n"
									+ "\t-QUIT: last but not least, the command for quit from this client.\n");
							break;
						case "QUIT":
							System.exit(0);
							break;
						default:
							System.out.println(input + ": Command not recognized.");
							break;
						}

					}
				} catch (IOException e) {
					System.err.println("An error occurred while reading the given list of known hosts.");
					System.exit(0);
				}
			} else {
				System.err.println("The given list of known nodes doesn't exists. Please re-launch the program with this command:\n java -jar PADfs-cli.jar list.conf");
				System.exit(0);
			}
		} else {
			System.err.println("The list of known nodes is missing. Please re-launch the program with this command:\n java -jar PADfs-cli.jar list.conf");
			System.exit(0);
		}

	}

}
