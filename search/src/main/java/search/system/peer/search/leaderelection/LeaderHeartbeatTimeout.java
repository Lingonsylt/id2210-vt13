package search.system.peer.search.leaderelection;

import common.peer.PeerAddress;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

import java.util.UUID;

public class LeaderHeartbeatTimeout extends Timeout {
    private final UUID requestID;
    private final PeerAddress elector;
	public LeaderHeartbeatTimeout(ScheduleTimeout request, PeerAddress elector) {
		super(request);
        requestID = UUID.randomUUID();
        this.elector = elector;
	}

    public UUID getRequestID() {
        return requestID;
    }

    public PeerAddress getElector() {
        return elector;
    }
}
