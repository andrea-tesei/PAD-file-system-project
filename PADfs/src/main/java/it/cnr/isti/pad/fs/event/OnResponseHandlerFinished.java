package it.cnr.isti.pad.fs.event;

import java.util.ArrayList;

/**
 * Interface for Response Handler finished Event. 
 * 
 * @author Andrea Tesei
 *
 */
public interface OnResponseHandlerFinished {
	/**
	 * Handler function called whenever a set of messages are handled.
	 * @param ids the list of messages which aren't processed due to some errors.
	 */
	void onFinishedHandleResponse(ArrayList<Integer> ids);
}
