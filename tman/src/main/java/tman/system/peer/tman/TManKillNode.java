package tman.system.peer.tman;

import common.peer.PeerAddress;
import se.sics.kompics.Event;

import java.util.ArrayList;


public class TManKillNode extends Event {
	PeerAddress node;

//-------------------------------------------------------------------
	public TManKillNode(PeerAddress node) {
		this.node = node;
	}

	public TManKillNode() {
	}

//-------------------------------------------------------------------
	public PeerAddress getNode() {
		return this.node;
	}
}
