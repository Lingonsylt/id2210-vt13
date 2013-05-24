package search.system.peer.search.indexadd;

import common.peer.PeerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timer;
import search.simulator.snapshot.Snapshot;
import search.system.peer.search.indexnextid.IndexNextIdService;
import search.system.peer.search.indexing.IndexingService;
import search.system.peer.search.Search;
import search.system.peer.search.leaderelection.LeaderElectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

public class IndexAddService {
    private static final Logger logger = LoggerFactory.getLogger(IndexAddService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Network> networkPort;
    Positive<Timer> timerPort;
    Search.TriggerDependency triggerDependency;
    IndexingService indexingService;
    IndexNextIdService indexNextIdService;
    LeaderElectionService leaderElectionService;

    // The peers closest to the top of the gradient, as discovered by cyclon. Used to route index add requests
    private ArrayList<PeerAddress> topmostCyclonPartners = new ArrayList<PeerAddress>();

    // The outstanding index add requests. Used for resending if timeout is reached
    private HashMap<UUID, IndexAddRequestMessage> outstandingLeaderRequests = new HashMap<UUID, IndexAddRequestMessage>();

    public IndexAddService(Search.TriggerDependency triggerDependency, LeaderElectionService leaderElectionService, IndexingService indexingService, IndexNextIdService indexNextIdService, PeerAddress self, Positive<Network> networkPort, Positive<Timer> timerPort) {
        this.timerPort = timerPort;
        this.triggerDependency = triggerDependency;
        this.self = self;
        this.networkPort = networkPort;
        this.indexingService = indexingService;
        this.leaderElectionService = leaderElectionService;
        this.indexNextIdService = indexNextIdService;
    }

    private void addEntryAtLeader(String key, String value) throws IOException {
        indexingService.addNewEntry(indexNextIdService.getIncrementedId(), key, value);
    }

    public void addEntryAtClient(String key, String value) {
        Snapshot.addIndexEntryInitiated();
        addEntryAtClient(key, value, null, UUID.randomUUID());
    }

    /**
     * Route an index add entry to the leader, or add it to the local index if we are the leader
     * If not called from the source of the request, relayFor will be used as sender address.
     * Else a retry will be made on timeout
     */
    private void addEntryAtClient(String key, String value, PeerAddress relayFor, UUID requestID) {
        if(leaderElectionService.isLeader()) {
            try {
                addEntryAtLeader(key, value);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
        } else {
            IndexAddRequestMessage message = null;
            PeerAddress recipient = null;
            if(recipient == null) {
                recipient = getTopmostPartner();
            }
            PeerAddress sender = relayFor == null ? self : relayFor;

            if (recipient == null) {
                message = new IndexAddRequestMessage(requestID, key, value, sender);
            } else {
                message = new IndexAddRequestMessage(requestID, key, value, sender, recipient);
            }

            ScheduleTimeout rst = new ScheduleTimeout(10000);
            rst.setTimeoutEvent(new IndexAddRequestMessageTimeout(rst, requestID, relayFor == null));
            triggerDependency.trigger(rst, timerPort);
            outstandingLeaderRequests.put(requestID, message);

            if (recipient != null) {
                Snapshot.addIndexEntryMessageSent();
                triggerDependency.trigger(message, networkPort);
            }
        }

    }

    /**
     * Add the index entry to the local index if we are the leader. Else route the message upwards in the gradient
     */
    public Handler<IndexAddRequestMessage> handleIndexAddRequestMessage = new Handler<IndexAddRequestMessage>() {
        @Override
        public void handle(IndexAddRequestMessage request) {
            if (leaderElectionService.isLeader()) {
                try {
                    addEntryAtLeader(request.getKey(), request.getValue());
                    Snapshot.addIndexEntryMessageSent();
                    triggerDependency.trigger(new LeaderResponseMessage(request.getRequestId(), self, request.getPeerSource()), networkPort);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            } else {
                addEntryAtClient(request.getKey(), request.getValue(), request.getPeerSource(), request.getRequestId());
            }
        }
    };

    /**
     * Remove the message from the "retry-table"
     */
    public Handler<LeaderResponseMessage> handleLeaderResponseMessage = new Handler<LeaderResponseMessage>() {
        @Override
        public void handle(LeaderResponseMessage response) {
            outstandingLeaderRequests.remove(response.getRequestId());
            Snapshot.addIndexEntryCompleted();
        }
    };

    /**
     * Return the tman/cyclon partner that's closest to the top of the gradient
     */
    private PeerAddress getTopmostPartner() {
        PeerAddress top = null;
        for(PeerAddress peer : topmostCyclonPartners) {
            if(top == null) {
                top = peer;
            } else {
                if (peer.compareTo(top) == -1) {
                    top = peer;
                }
            }
        }

        for(PeerAddress peer : leaderElectionService.getTManPartners()) {
            if(top == null) {
                top = peer;
            } else {
                if (peer.compareTo(top) == -1) {
                    top = peer;
                }
            }
        }

        if (top == null || self.compareTo(top) <= 0) {
            return null;
        }
        return top;
    }

    /**
     * Collect cyclon samples for more efficient routing to the leader
     */
    public void receiveCyclonSample (List<PeerAddress> sample) {
        addTopMostCyclonPartners(sample);
    }

    /**
     * Collect the best cyclon samples for more efficient routing to the leader
     */
    private void addTopMostCyclonPartners(List<PeerAddress> parnters) {
        for(PeerAddress peer : parnters) {
            if (topmostCyclonPartners.contains(peer)) {
                continue;
            }
            if (topmostCyclonPartners.size() < 10) {
                topmostCyclonPartners.add(peer);
            } else {
                PeerAddress bottom = getBottomCyclonPartner();
                if(peer.compareTo(bottom) == -1) {
                    topmostCyclonPartners.remove(bottom);
                    topmostCyclonPartners.add(peer);
                }
            }
        }
    }

    /**
     * Get the worst ranked top-cyclon partner
     */
    private PeerAddress getBottomCyclonPartner() {
        PeerAddress bottom = null;
        for(PeerAddress peer : topmostCyclonPartners) {
            if(bottom == null) {
                bottom = peer;
            } else {
                if (peer.compareTo(bottom) == 1) {
                    bottom = peer;
                }
            }
        }
        return bottom;
    }

    /**
     * Retry a IndexAddRequestMessage if it reached timeout and we are the original source of the request
     * If not, just drop it
     * In both cases, remove the failed peer from the topmost cyclon parnters and from the tman view
     */
    public Handler<IndexAddRequestMessageTimeout> handleIndexAddRequestMessageTimeout = new Handler<IndexAddRequestMessageTimeout>() {
        public void handle(IndexAddRequestMessageTimeout message) {
            IndexAddRequestMessage outstanding = outstandingLeaderRequests.get(message.getRequestID());
            if (outstanding != null) {
                topmostCyclonPartners.remove(outstanding.getPeerDestination());
                leaderElectionService.getTManPartners().remove(outstanding.getPeerDestination());
                if (message.getRetry()) {
                    addEntryAtClient(outstanding.getKey(), outstanding.getValue());
                }
            }
        }
    };
}
