package search.system.peer.search;

import common.peer.PeerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import search.simulator.snapshot.Snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

public class IndexNextIdService {
    private static final Logger logger = LoggerFactory.getLogger(IndexNextIdService.class);

    // If leader, the next id that should be used when adding an entry to lucene
    private int nextId = 0;

    public IndexNextIdService() {
    }

    public int getIncrementedId() {
        return ++nextId;
    }

    public int getNextId() {
        return nextId;
    }

    public void setNextId(int nextId) {
        this.nextId = nextId;
    }
}
