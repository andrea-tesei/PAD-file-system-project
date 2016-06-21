package it.cnr.isti.pad.fs.runnables;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListSet;

import it.cnr.isti.pad.fs.event.OnResponseHandlerFinished;
import it.cnr.isti.pad.fs.storage.StorageNode;
import it.cnr.isti.pad.fs.udpsocket.Message;
import it.cnr.isti.pad.fs.udpsocket.StorageMessage;

public class StorageResponseAsyncHandler implements Runnable {

	public static ConcurrentSkipListSet<Integer> idsToHandle = new ConcurrentSkipListSet<Integer>();
	
	private OnResponseHandlerFinished listener = null;
	
	public void addListener(OnResponseHandlerFinished listen){
		this.listener = listen;
	}
	
	private void triggerListeners(ArrayList<Integer> idsNotHandled){
		if(listener != null)
			listener.onFinishedHandleResponse(idsNotHandled);
	}

	public void addIdsToHandle(ArrayList<Integer> ids) {
		ids.forEach(id -> idsToHandle.add(id));
	}
	
	public void addSingleIdToQueue(Integer id){
		StorageResponseAsyncHandler.idsToHandle.add(id);
	}

	@Override
	public void run() {
		ArrayList<Integer> returnedList = new ArrayList<Integer>();
		idsToHandle.forEach(idrequest -> 
		{
			int attempts = 0;
			while(StorageNode.pendingRequest.containsKey(idrequest) && attempts < 20){
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				attempts++;
			}
			StorageMessage receivedMsg = StorageNode.pendingResponse.get(idrequest);
			if(receivedMsg != null){
				StorageResponseAsyncHandler.idsToHandle.remove(idrequest);
				if(receivedMsg.getReturnCode() == Message.ReturnCode.OK){
					if(receivedMsg.getCommand() == Message.Command.PUT)
						StorageNode.LOGGER.info("The file " + receivedMsg.getFileName() + " has been successfully saved in " + receivedMsg.getHost());
					else if(receivedMsg.getCommand() == Message.Command.UPDATE_BACKUP)
						StorageNode.LOGGER.info("The backup file " + receivedMsg.getFileName() + " has been successfully saved in " + receivedMsg.getHost());
					else if(receivedMsg.getCommand() == Message.Command.CONFLICT_RESOLUTION)
						StorageNode.LOGGER.info("The conflict resolution has been completed in replica/remote node.");
				} else if(receivedMsg.getReturnCode() == Message.ReturnCode.ERROR){
					if(receivedMsg.getCommand() == Message.Command.PUT)
						StorageNode.LOGGER.info("The system encountered a problem while PUT file " + receivedMsg.getFileName() + " in " + receivedMsg.getHost());
					else if(receivedMsg.getCommand() == Message.Command.UPDATE_BACKUP)
						StorageNode.LOGGER.info("The system encountered a problem while UPDATE_BACKUP file " + receivedMsg.getFileName() + " in " + receivedMsg.getHost());
					else if(receivedMsg.getCommand() == Message.Command.CONFLICT_RESOLUTION)
						StorageNode.LOGGER.info("The replica node fails during CONFLICT_RESOLUTION for file " + receivedMsg.getFileName() + " in " + receivedMsg.getHost());
				}
			} else
				returnedList.add(idrequest);
		});
		if(!returnedList.isEmpty())
			this.triggerListeners(returnedList);
	}

}
