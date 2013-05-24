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

import java.math.BigInteger;
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

    public void receiveTManSample(ArrayList<PeerAddress> tmanSample) {
        tmanPartners = tmanSample;

        checkForLeadership();
        heartbeatElectors();
        updateAliveElectorsStatus();
        tmanPartnersLastRound = new ArrayList<PeerAddress>(tmanPartners);
    }

    public Handler<LeaderHeartbeatTimeout> handleLeaderHeartbeatTimeout = new Handler<LeaderHeartbeatTimeout>() {
        public void handle(LeaderHeartbeatTimeout message) {
            if (outstandingElectorHearbeats.containsKey(message.getRequestID())) {
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    if (message.getElector().getPeerId().equals(BigInteger.ONE)) {
                        //System.out.println("One is down!");
                    } else {
                        //System.out.println(".");
                    }


                }

                if (message.getElector().equals(leader)) {
                    leader = null;
                    if (message.getElector().getPeerId().equals(new BigInteger("2"))) {
                        //System.out.println("Leader is dead!");
                    }
                    //System.out.println("Leader heartbeat timeout! Initiating leader election: " + self.getPeerId());
                    //System.out.println("Removing " + message.getElector().getPeerId() + ": " + tmanPartners.remove(message.getElector()));
                    initiateLeaderElection();
                }
                aliveElectors.put(message.getElector(), false);
                outstandingElectorHearbeats.remove(message.getRequestID());
                triggerDependency.trigger(new TManKillNode(message.getElector()), tmanSamplePort);
            }
        }
    };

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
            //System.out.println(self + ": leader election started!");
            isRunningElection = true;
            electionYesVotes = 0;
            electionParticipants = tmanPartners.size();

            for(PeerAddress neighbor : tmanPartners) {
                triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "AM_I_LEGEND", self, neighbor), networkPort);
            }
        }
    }

    public void heartbeatElectors() {
        if (leader != null && !isLeader) {
            if (self.getPeerId().equals(new BigInteger("2"))) {
                //System.out.println("Hearbeating nr 2!");
            }

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
                    //leaderHeartbeatOutstanding = timeoutMessage.getRequestID();
                    triggerDependency.trigger(rst, timerPort);
                    triggerDependency.trigger(new LeaderElectionMessage(timeoutMessage.getRequestID(), "ARE_YOU_ALIVE", self, elector), networkPort);
                }
            }
        }
    }

    public void checkForLeadership() {
        if(isRunningElection) {
            return;
        }

        if (isLeader) {
            if (!isLowestPeer(self)) {
                isLeader = false;
            }
        } else {
            if (leader != null && !isLowestPeer(leader)) {
                triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, leader), networkPort);
            }
        }

        if (tmanPartnersLastRound.equals(tmanPartners)) {

            sameNeighborsRoundCount++;
        } else {
            sameNeighborsRoundCount = 0;
        }
        if (self.getPeerId().equals(new BigInteger("2"))) {
            //System.out.println("2: sameNeighborsRoundCount: " + sameNeighborsRoundCount);
        }

        if (tmanPartners.size() == TMan.C && sameNeighborsRoundCount >= 3) {
            if (isLowestPeer(self)) {
                initiateLeaderElection();
            } else {
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    //System.out.println("2: not lowest peer: " + tmanPartners + ", " + outstandingElectorHearbeats + " ### " + aliveElectors);
                }
            }
        }
    }

    public boolean isLowestPeer(PeerAddress peer) {
        return isLowestPeer(peer, tmanPartners);
    }

    public boolean isLowestPeer(PeerAddress peer, List<PeerAddress> partners) {
        for(PeerAddress neighbor : partners) {
            if (aliveElectors.containsKey(neighbor) && aliveElectors.get(neighbor) && neighbor.getPeerId().compareTo(peer.getPeerId()) == -1) {
                return false;
            }
        }
        return true;
    }

    public void announceLeadership() {
        isLeader = true;
        isRunningElection = false;

        for(PeerAddress neighbor : tmanPartners) {
            //System.out.println("Announce: " + neighbor);
            triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "I_AM_LEGEND", self, neighbor), networkPort);
        }
    }

    public Handler<LeaderElectionMessage> handleLeaderElectionIncoming = new Handler<LeaderElectionMessage>() {
        @Override
        public void handle(LeaderElectionMessage message) {
            if (message.getCommand().equals("AM_I_LEGEND")) {
                if (isLowestPeer(message.getPeerSource())) {
                    triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LEGEND", indexingService.getMaxLuceneIndex(), self, message.getPeerSource()), networkPort);
                } else {
                    //if (self.getPeerId().equals(new BigInteger("2")) && message.getPeerSource().getPeerId().equals(new BigInteger("1"))) {
                    if (message.getPeerSource().getPeerId().equals(new BigInteger("2"))) {
                        //System.out.println(self.getPeerId() + " says " + message.getPeerSource().getPeerId()  + " is not lowest: " + prettyPrintPeerAddressesList(tmanPartners));
                    }
                    triggerDependency.trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, message.getPeerSource()), networkPort);
                }
            } else if (message.getCommand().equals("YOU_ARE_LEGEND")) {
                if (isRunningElection) {
                    electionYesVotes++;
                    if (message.getNextId() > indexNextIdService.getNextId()) {
                        indexNextIdService.setNextId(message.getNextId());
                    }
                    if (electionYesVotes > electionParticipants / 2) {
                        announceLeadership();
                    }
                }
            } else if (message.getCommand().equals("I_AM_LEGEND")) {

                leader = message.getPeerSource();
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    //System.out.println("New leader!" + leader);
                }
            } else if (message.getCommand().equals("YOU_ARE_LOSER")) {
                if (isLeader) {
                    //System.out.println("I AM LOSER! " + self.getPeerId());
                    isLeader = false;
                }
                isRunningElection = false;
            } else if (message.getCommand().equals("ARE_YOU_ALIVE")) {
                if (self.getPeerId().equals(new BigInteger("1"))) {
                    //System.out.println("1: alive");
                }
                triggerDependency.trigger(new LeaderElectionMessage(message.getRequestId(), "I_AM_ALIVE", self, message.getPeerSource()), networkPort);
            } else if (message.getCommand().equals("I_AM_ALIVE")) {
                if(outstandingElectorHearbeats.get(message.getRequestId()) != null) {
                    outstandingElectorHearbeats.remove(message.getRequestId());
                    aliveElectors.put(message.getPeerSource(), true);
                }
            }
        }
    };
}
