package search.system.peer.search;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

import java.util.UUID;

public class LeaderRequestMessageTimeout extends Timeout {
    private final UUID requestID;

	public LeaderRequestMessageTimeout(ScheduleTimeout request, UUID requestID) {
		super(request);
        this.requestID = requestID;
	}

    public UUID getRequestID() {
        return requestID;
    }
}
