package search.system.peer.search;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

import java.util.UUID;

public class LeaderRequestMessageTimeout extends Timeout {
    private final UUID requestID;

	public LeaderRequestMessageTimeout(SchedulePeriodicTimeout request, UUID requestID) {
		super(request);
        this.requestID = requestID;
	}

    public UUID getRequestID() {
        return requestID;
    }
}
