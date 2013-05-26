package search.system.peer.search.leaderelection;

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
import tman.system.peer.tman.TMan;
import tman.system.peer.tman.TManKillNode;
import tman.system.peer.tman.TManSamplePort;

import java.util.*;

public class LeaderElectionService {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Network> networkPort;
    Search.TriggerDependency triggerDependency;
    IndexingService indexingService;
    Positive<TManSamplePort> tmanSamplePort;
    Positive<Timer> timerPort;
    IndexNextIdService indexNextIdService;

    private boolean isLeader = false;
    private boolean isRunningElection = false;
    private int electionYesVotes = 0;
    private int electionParticipants = 0;

    // If a part of the quorum, the current leader
    private PeerAddress leader = null;


    // The gradient neighbors
    ArrayList<PeerAddress> tmanPartners = new ArrayList<PeerAddress>();

    // The gradient neighbors last round, to be compared against current
    private ArrayList<PeerAddress> tmanPartnersLastRound = new ArrayList<PeerAddress>();

    // The number of rounds the gradient neighbors have been the same
    private int sameNeighborsRoundCount = 0;

    // The outstanding requests to peers in the elector group. Used to detect failure of leader and leader candidates
    private HashMap<UUID, PeerAddress> outstandingElectorHearbeats = new HashMap<UUID, PeerAddress>();

    // An up-to-date map describing the dead/alive state of the leader and leader candidates
    private HashMap<PeerAddress, Boolean> aliveElectors = new HashMap<PeerAddress, Boolean>();

    public LeaderElectionService(Search.TriggerDependency triggerDependency, IndexingService indexingService, IndexNextIdService indexNextIdService, PeerAddress self, Positive<TManSamplePort> tmanSamplePort, Positive<Network> networkPort, Positive<Timer> timerPort) {
        this.timerPort = timerPort;
        this.triggerDependency = triggerDependency;
        this.self = self;
        this.networkPort = networkPort;
        this.indexingService = indexingService;
        this.tmanSamplePort = tmanSamplePort;
        this.indexNextIdService = indexNextIdService;
    }

    /**
     * Receive a TMan sample from the Search-layer
     */
    public void receiveTManSample(ArrayList<PeerAddress> tmanSample) {
        tmanPartners = tmanSample;

        checkForLeadership();
        heartbeatElectors();
        updateAliveElectorsStatus();
        tmanPartnersLastRound = new ArrayList<PeerAddress>(tmanPartners);
    }

    /**
     * Receive a timeout and mark the elector as dead in the aliveElectors-map
     * If the dead peer is the current leader, initiate a leader election
     */
    public Handler<LeaderHeartbeatTimeout> handleLeaderHeartbeatTimeout = new Handler<LeaderHeartbeatTimeout>() {
        public void handle(LeaderHeartbeatTimeout message) {
            if (outstandingElectorHearbeats.containsKey(message.getRequestID())) {
                if (message.getElector().equals(leader)) {
                    leader = null;
                    initiateLeaderElection();
                }
                aliveElectors.put(message.getElector(), false);
                outstandingElectorHearbeats.remove(message.getRequestID());

                // Tell the TMan layer that this node has been marked as failed
                triggerDependency.trigger(new TManKillNode(message.getElector()), tmanSamplePort);
            }
        }
    };

    /**
     * Add newly discovered peers to the aliveElectors-map, and mark them as alive
     */
    void updateAliveElectorsStatus() {
        for(PeerAddress peer : tmanPartners) {
            if(!aliveElectors.containsKey(peer)) {
                aliveElectors.put(peer, true);
            }
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public List<PeerAddress> getTManPartners() {
        return tmanPartners;
    }

    public void initiateLeaderElection() {
        if (!isLeader && !isRunningElection) {
            Snapshot.leaderElectionStarted();
            isRunningElection = true;
            electionYesVotes = 0;
            electionParticipants = tmanPartners.size();

            for(PeerAddress neighbor : tmanPartners) {
                Snapshot.leaderElectionMessageSent();
                triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "AM_I_LEGEND", self, neighbor), networkPort);
            }
        }
    }

    /**
     * Send a heartbeat to all nodes that are leader candidates, and the leader, to detect failed nodes
     */
    public void heartbeatElectors() {
        if (leader != null && !isLeader) {
            List<PeerAddress> electors = new ArrayList<PeerAddress>(tmanPartners);
            if (!electors.contains(leader)) {
                electors.add(leader);
            }

            for (PeerAddress elector : tmanPartners) {
                if (!outstandingElectorHearbeats.values().contains(elector)) {
                    ScheduleTimeout rst = new ScheduleTimeout(1000);
                    LeaderHeartbeatTimeout timeoutMessage = new LeaderHeartbeatTimeout(rst, elector);
                    rst.setTimeoutEvent(timeoutMessage);
                    outstandingElectorHearbeats.put(timeoutMessage.getRequestID(), elector);
                    triggerDependency.trigger(rst, timerPort);
                    Snapshot.leaderElectionMessageSent();
                    triggerDependency.trigger(new LeaderElectionMessage(timeoutMessage.getRequestID(), "ARE_YOU_ALIVE", self, elector), networkPort);
                }
            }
        }
    }

    /**
     * Check if we have converged in the gradient, and trigger a leader election if we have, and are on the top of the gradient
     * Check if we're still on the top of the gradient in if we are the leader
     * Check if the leader is on top of the gradient and tell it if it's not
     */
    public void checkForLeadership() {
        if(isRunningElection) {
            return;
        }

        // Demote ourselves if we find that we are not supposed to be leader anymore
        if (isLeader) {
            if (!isLowestPeer(self)) {
                isLeader = false;
            }

        // Give a heads up to the leader, if we detect that it shouldn't be leader anymore
        } else {
            if (leader != null && !isLowestPeer(leader)) {
                Snapshot.leaderElectionMessageSent();
                triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, leader), networkPort);
            }
        }

        // Trigger a leader election if we have converged in the gradient for three rounds in a row
        if (tmanPartnersLastRound.equals(tmanPartners)) {
            sameNeighborsRoundCount++;
        } else {
            sameNeighborsRoundCount = 0;
        }
        if (tmanPartners.size() == TMan.VIEW_SIZE && sameNeighborsRoundCount >= 3) {
            if (isLowestPeer(self)) {
                initiateLeaderElection();
            }
        }
    }

    /**
     * Return true if peer has a lower id than all alive TMan partners
     */
    public boolean isLowestPeer(PeerAddress peer) {
        return isLowestPeer(peer, tmanPartners);
    }

    /**
     * Return true if peer has a lower id than all alive partners
     */
    public boolean isLowestPeer(PeerAddress peer, List<PeerAddress> partners) {
        for(PeerAddress neighbor : partners) {
            if (aliveElectors.containsKey(neighbor) && aliveElectors.get(neighbor) && neighbor.getPeerId().compareTo(peer.getPeerId()) == -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Announce to the electors that we are the leader
     */
    public void announceLeadership() {
        isLeader = true;
        isRunningElection = false;

        for(PeerAddress neighbor : tmanPartners) {
            triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "I_AM_LEGEND", self, neighbor), networkPort);
        }
    }

    /**
     * Handle all incoming leader election control messages
     * TODO: Refactor this out to multiple methods
     * Messages:
     *   AM_I_LEGEND: Request for a leader election vote
     *   I_AM_LEGEND: The sender is announcing it's the new leader
     *   YOU_ARE_LEGEND: Yes vote in leader election
     *   YOU_ARE_LOSER: No vote in leader election
     *   ARE_YOU_ALIVE: Heartbeat request
     *   I_AM_ALIVE: Heartbeat response
     */
    public Handler<LeaderElectionMessage> handleLeaderElectionIncoming = new Handler<LeaderElectionMessage>() {
        @Override
        public void handle(LeaderElectionMessage message) {
            // AM_I_LEGEND: Request for a leader election vote
            if (message.getCommand().equals("AM_I_LEGEND")) {
                if (isLowestPeer(message.getPeerSource())) {
                    // YOU_ARE_LEGEND: Yes vote in leader election
                    Snapshot.leaderElectionMessageSent();
                    triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LEGEND", indexingService.getMaxLuceneIndex(), self, message.getPeerSource()), networkPort);
                } else {
                    // YOU_ARE_LOSER: No vote in leader election
                    Snapshot.leaderElectionMessageSent();
                    triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, message.getPeerSource()), networkPort);
                }
            // YOU_ARE_LEGEND: Yes vote in leader election
            } else if (message.getCommand().equals("YOU_ARE_LEGEND")) {
                // Collect votes, until a majority votes Yes, or until one votes No
                if (isRunningElection) {
                    electionYesVotes++;
                    if (message.getNextId() > indexNextIdService.getNextId()) {
                        indexNextIdService.setNextId(message.getNextId());
                    }
                    if (electionYesVotes > electionParticipants / 2) {
                        announceLeadership();
                    }
                }
            // I_AM_LEGEND: The sender is announcing it's the new leader
            } else if (message.getCommand().equals("I_AM_LEGEND")) {

                leader = message.getPeerSource();
            // YOU_ARE_LOSER: No vote in leader election
            } else if (message.getCommand().equals("YOU_ARE_LOSER")) {
                if (isLeader) {
                    isLeader = false;
                }
                isRunningElection = false;
            // ARE_YOU_ALIVE: Heartbeat request
            } else if (message.getCommand().equals("ARE_YOU_ALIVE")) {
                triggerDependency.trigger(new LeaderElectionMessage(message.getRequestId(), "I_AM_ALIVE", self, message.getPeerSource()), networkPort);
            // I_AM_ALIVE: Heartbeat response
            } else if (message.getCommand().equals("I_AM_ALIVE")) {
                if(outstandingElectorHearbeats.get(message.getRequestId()) != null) {
                    outstandingElectorHearbeats.remove(message.getRequestId());
                    aliveElectors.put(message.getPeerSource(), true);
                }
            }
        }
    };
}
