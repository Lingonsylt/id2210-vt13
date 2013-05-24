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
    private HashMap<UUID, LeaderRequestMessage> outstandingLeaderRequests = new HashMap<UUID, LeaderRequestMessage>();

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

    private void addEntryAtClient(String key, String value, PeerAddress relayFor, UUID requestID) {
        if(leaderElectionService.isLeader()) {
            try {
                addEntryAtLeader(key, value);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
        } else {
            LeaderRequestMessage message = null;
            PeerAddress recipient = null; //leader;
            if(recipient == null) {
                recipient = getTopmostPartner();
            }
            PeerAddress sender = relayFor == null ? self : relayFor;

            if (recipient == null) {
                message = new LeaderRequestMessage(requestID, key, value, sender);
            } else {
                message = new LeaderRequestMessage(requestID, key, value, sender, recipient);
            }

            ScheduleTimeout rst = new ScheduleTimeout(10000);
            rst.setTimeoutEvent(new LeaderRequestMessageTimeout(rst, requestID, relayFor == null));
            triggerDependency.trigger(rst, timerPort);
            outstandingLeaderRequests.put(requestID, message);

            if (recipient != null) {
                //System.out.println(self + ": Adding at: " + recipient);
                Snapshot.addIndexEntryMessageSent();
                triggerDependency.trigger(message, networkPort);
            }
        }

    }

    public Handler<LeaderRequestMessage> handleLeaderRequestMessage = new Handler<LeaderRequestMessage>() {
        @Override
        public void handle(LeaderRequestMessage request) {
            if (leaderElectionService.isLeader()) {
                try {
                    addEntryAtLeader(request.getKey(), request.getValue());
                    //System.out.println(self.getPeerId() + ": add response to " + request.getPeerSource());
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

    public Handler<LeaderResponseMessage> handleLeaderResponseMessage = new Handler<LeaderResponseMessage>() {
        @Override
        public void handle(LeaderResponseMessage response) {
            //System.out.println(self.getPeerId() + ": Successfully added an entry at leader!");
            outstandingLeaderRequests.remove(response.getRequestId());
            Snapshot.addIndexEntryCompleted();
        }
    };

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

    public void receiveCyclonSample (List<PeerAddress> sample) {
        addTopMostCyclonPartners(sample);
    }

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

    public Handler<LeaderRequestMessageTimeout> handleLeaderRequestMessageTimeout = new Handler<LeaderRequestMessageTimeout>() {
        public void handle(LeaderRequestMessageTimeout message) {
            LeaderRequestMessage outstanding = outstandingLeaderRequests.get(message.getRequestID());
            if (outstanding != null) {
                //System.out.println(self.getPeerId() + ": Request failed towards " + outstanding.getPeerDestination());
                topmostCyclonPartners.remove(outstanding.getPeerDestination());
                leaderElectionService.getTManPartners().remove(outstanding.getPeerDestination());
                if (message.getRetry()) {
                    if (leaderElectionService.getTManPartners().contains(outstanding.getPeerDestination()) || topmostCyclonPartners.contains(outstanding.getPeerDestination())) {
                        throw new RuntimeException("Remove failed!");
                    }
                    //System.out.println(self.getPeerId() + ": Retrying request " + outstanding.getRequestId());
                    addEntryAtClient(outstanding.getKey(), outstanding.getValue());
                }
            } else {
                //System.out.println("Succeeded with request " + message.getRequestID() + " at leader " + leader);
            }
        }
    };
}
