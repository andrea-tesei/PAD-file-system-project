package com.google.code.gossip.manager;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.gossip.GossipService;
import com.google.code.gossip.LocalGossipMember;

/**
 * [The active thread: periodically send gossip request.] The class handles
 * gossiping the membership list. This information is important to maintaining a
 * common state among all the nodes, and is important for detecting failures.
 */
abstract public class ActiveGossipThread implements Runnable 
{
	private final GossipManager gossipManager;

	private final AtomicBoolean keepRunning;

	public ActiveGossipThread(final GossipManager gossipManager) 
	{
		this.gossipManager = gossipManager;
		this.keepRunning = new AtomicBoolean(true);
	}

	@Override
	public void run() 
	{
		while (keepRunning.get()) {
			try {
				TimeUnit.MILLISECONDS.sleep(gossipManager.getSettings().getGossipInterval());
				sendMembershipList(gossipManager.getMyself(), gossipManager.getMemberList());
			} catch (InterruptedException e) {
				GossipService.LOGGER.error(e);
				keepRunning.set(false);
			}
		}
		shutdown();
	}

	public void shutdown() 
	{
		keepRunning.set(false);
	}

	/**
	 * Performs the sending of the membership list, after we have incremented
	 * our own heartbeat.
	 *
	 * @param me			The local member of the thread, used to update its heartbeat.
	 * @param memberList	The list of members which are stored in the local list of members.
	 */
	abstract protected void sendMembershipList(LocalGossipMember me, final List<LocalGossipMember> memberList);

	/**
	 * Abstract method which should be implemented by a subclass. This method
	 * should return a member of the list to gossip with.
	 *
	 * @param memberList	The list of members which are stored in the local list of members.
	 * 
	 * @return The chosen LocalGossipMember to gossip with.
	 */
	abstract protected LocalGossipMember selectPartner(final List<LocalGossipMember> memberList);
}
