package search.system.peer.search;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

import java.util.UUID;

public class LeaderRequestMessageTimeout extends Timeout {
    private final UUID requestID;
    private final boolean retry;

	public LeaderRequestMessageTimeout(ScheduleTimeout request, UUID requestID, boolean retry) {
		super(request);
        this.requestID = requestID;
        this.retry = retry;
	}

    public UUID getRequestID() {
        return requestID;
    }

    public boolean getRetry() {
        return retry;
    }
}
