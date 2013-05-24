package search.system.peer.search.web;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class InspectTrigger extends Timeout {
	public InspectTrigger(ScheduleTimeout request) {
		super(request);
	}
}
