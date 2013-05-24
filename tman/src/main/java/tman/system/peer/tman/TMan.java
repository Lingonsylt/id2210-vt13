package tman.system.peer.tman;

import common.configuration.CyclonConfiguration;
import common.configuration.TManConfiguration;
import common.peer.PeerAddress;

import java.math.BigInteger;
import java.util.*;

import cyclon.system.peer.cyclon.*;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

import tman.simulator.snapshot.Snapshot;

public final class TMan extends ComponentDefinition {

    Random randomGenerator;
    private CyclonConfiguration cyclonConfiguration;

    // Ports
    Negative<TManSamplePort> tmanPartnersPort = negative(TManSamplePort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);

    // Myself
    private PeerAddress self;

    // Outstanding requests to exchange addresses
    // TODO: The reaction to timeouts is not implemented
    private HashMap<UUID, PeerAddress> outstandingRequests;

    // The current partner addresses
    private ArrayList<PeerAddress> tmanPartners;

    // Last seen cyclon sample. Used to combat the asynchronicity. Might be a better way
    private ArrayList<PeerAddress> lastSeenCyclonPartners;

    public static final int VIEW_SIZE = 6;

    public class TManSchedule extends Timeout {
        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public TMan() {
        tmanPartners = new ArrayList<PeerAddress>();
        outstandingRequests = new HashMap<UUID, PeerAddress>();
        randomGenerator = new Random();
        lastSeenCyclonPartners = new ArrayList<PeerAddress>();

        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
        subscribe(handleTManKillNode, tmanPartnersPort);
    }

    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {
            self = init.getSelf();
            TManConfiguration tmanConfiguration = init.getConfiguration();
            cyclonConfiguration = init.getCyclonConfiguration();
            Snapshot.addPeer(self);

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(tmanConfiguration.getPeriod(), tmanConfiguration.getPeriod());
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);
        }
    };

    /**
     * Comparator used to rank peers with lower id, but closer to oneself, higher - and peers with higher id
     * and further away ranked lower
     */
    Comparator<PeerAddress> rankingComparator = new Comparator<PeerAddress>() {
        public int compare(PeerAddress left, PeerAddress right) {
            // Two null peers are ranked the same
            if (left.getPeerId() == null && right.getPeerId() == null) {
                return 0;

            // Non-null peer ranked higher than null-peer
            } else if (left.getPeerId() == null) {
                return -1;
                // Non-null peer ranked higher than null-peer
            } else if (right.getPeerId() == null) {
                return 1;
            } else {
                // If both peers have higher id than us, the closest is better
                if (left.getPeerId().compareTo(self.getPeerId()) > 0 && right.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return left.compareTo(right);

                // If left peer is higher than us and right peer is lower. Right peer has better rank
                } else if (left.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return 1;

                // If right peer is higher than us and left peer is lower. Left peer has better rank
                } else if (right.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return -1;

                // If both peers are lower than us, the closest is better
                } else {
                    return right.compareTo(left);
                }
            }
        }};

    /**
     * Add a number of peers to a buffer, omitting specified peers and duplicates
     */
    void addUniqueToBufferOmitting(List<PeerAddress> add, List<PeerAddress> buffer, PeerAddress... omitting) {
        addUniqueToBufferOmitting(add, buffer, false, omitting);
    }

    /**
     * Add a number of peers to a buffer, omitting specified peers and duplicates.
     * Keeping buffer size below or equal to VIEW_SIZE if required
     */
    void addUniqueToBufferOmitting(List<PeerAddress> add, List<PeerAddress> buffer, boolean keepRoof, PeerAddress... omitting) {
        for (PeerAddress peerAddress : add) {
            if (keepRoof && buffer.size() == VIEW_SIZE) {
                break;
            }
            if (!buffer.contains(peerAddress)) {
                boolean omit = false;
                for (PeerAddress omitPeer : omitting) {
                    if (peerAddress.equals(omitPeer)) {
                        omit = true;
                        break;
                    }
                }
                if(!omit) {
                    buffer.add(peerAddress);
                }
            }
        }
    }

    /**
     * Select a random peer among the top half of the ranked peers
     * A peer close to us
     */
    PeerAddress selectPeer(List<PeerAddress> view) {
        List<PeerAddress> sortCopy = new ArrayList<PeerAddress>(view);
        Collections.sort(sortCopy, rankingComparator);
        int halfBufferSize = sortCopy.size() / 2;
        int randomPeerIndex = randomGenerator.nextInt(halfBufferSize + 1);
        return sortCopy.get(randomPeerIndex);
    }

    /**
     * Pick the top VIEW_SIZE peers among the ranked peers
     */
    ArrayList<PeerAddress> selectView(List<PeerAddress> view) {
        List<PeerAddress> sortCopy = new ArrayList<PeerAddress>(view);
        Collections.sort(sortCopy, rankingComparator);

        ArrayList<PeerAddress> result = new ArrayList<PeerAddress>();
        int stopAt = Math.min(VIEW_SIZE, view.size());
        for (int i = 0; i < stopAt; i++) {
            result.add(sortCopy.get(i));
        }
        return result;

    }

    /**
     * Receive a cyclon sample, update statistics, trigger a tman partners request and publish tman sample
     */
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            // Don't start the simulation until all peers have joined. Needed to make experiment results comparable
            if(!Snapshot.hasAllPeersJoined()) {
                return;
            }

            ArrayList<PeerAddress> cyclonPartners = event.getSample();
            lastSeenCyclonPartners = new ArrayList<PeerAddress>(cyclonPartners);

            // Report statistics
            Snapshot.updateTManPartners(self, tmanPartners);
            Snapshot.updateCyclonPartners(self, cyclonPartners);

            // Initiate a partners request
            triggerTManPartnersRequest(cyclonPartners);

            // Publish sample to connected components
            trigger(new TManSample(tmanPartners), tmanPartnersPort);
        }
    };

    /**
     * Set up a TMan partners request and send it to a peer close in rank to us
     */
    private void triggerTManPartnersRequest(List<PeerAddress> cyclonPartners) {
        if (tmanPartners.size() != 0) {
            // Select a peer close to us
            PeerAddress receivingPeer = selectPeer(tmanPartners);

            // Set up a buffer with our neighbors, remove the receiver and add self
            List<PeerAddress> buffer = new ArrayList<PeerAddress>(tmanPartners);
            buffer.remove(receivingPeer);
            buffer.add(self);

            // Add a cyclon sample to the buffer, omitting the receiving peer to avoid duplicates
            addUniqueToBufferOmitting(cyclonPartners, buffer, receivingPeer);

            // Set up a timeout for the request, to detect failed communication
            // TODO: The reaction to timeouts is not implemented
            ScheduleTimeout rst = new ScheduleTimeout(cyclonConfiguration.getShuffleTimeout());
            rst.setTimeoutEvent(new ShuffleTimeout(rst, receivingPeer));
            UUID rTimeoutId = rst.getTimeoutEvent().getTimeoutId();
            outstandingRequests.put(rTimeoutId, receivingPeer);

            // Convert the buffer to this strange format
            ArrayList<PeerDescriptor> bufferDescriptors = new ArrayList<PeerDescriptor>();
            for (PeerAddress peer : buffer) {
                bufferDescriptors.add(new PeerDescriptor(peer));
            }
            DescriptorBuffer descriptorBuffer = new DescriptorBuffer(self, bufferDescriptors);

            // Send the buffer to the receiver
            trigger(new ExchangeMsg.Request(rTimeoutId, descriptorBuffer, self, receivingPeer), networkPort);
        } else {
            // If our tman view is completely empty, fill it with cyclon samples
            addUniqueToBufferOmitting(cyclonPartners, tmanPartners, true, self);
        }
    }

    /**
     * Handle a TMan partners exchange request. Select a buffer to send, send it
     * and incorporate the received buffer into the local view
     */
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            ArrayList<PeerAddress> cyclonPartners = new ArrayList<PeerAddress>(lastSeenCyclonPartners);

            // Set up a buffer with our neighbors, remove the receiver and add self
            List<PeerAddress> buffer = new ArrayList<PeerAddress>(tmanPartners);
            buffer.add(self);
            buffer.remove(event.getPeerSource());

            // Add a cyclon sample to the buffer, omitting the receiving peer to avoid duplicates
            addUniqueToBufferOmitting(cyclonPartners, buffer, event.getPeerSource());

            // Convert the buffer to this strange format
            ArrayList<PeerDescriptor> bufferDescriptors = new ArrayList<PeerDescriptor>();
            for (PeerAddress peer : buffer) {
                bufferDescriptors.add(new PeerDescriptor(peer));
            }
            DescriptorBuffer descriptorBuffer = new DescriptorBuffer(self, bufferDescriptors);

            // Send the buffer to the receiver
            trigger(new ExchangeMsg.Response(event.getRequestId(), descriptorBuffer, self, event.getPeerSource()), networkPort);

            // Convert the buffer from the strange format to an ArrayList<PeerAddress>
            ArrayList<PeerAddress> remoteAddresses = new ArrayList<PeerAddress>();
            for (PeerDescriptor descriptor : event.getRandomBuffer().getDescriptors()) {
                remoteAddresses.add(descriptor.getPeerAddress());
            }

            // Merge our view and the received view and select the VIEW_SIZE highest ranked peers ar our new view
            List<PeerAddress> newTmanPartners = new ArrayList<PeerAddress>(tmanPartners);
            addUniqueToBufferOmitting(remoteAddresses, newTmanPartners, self);
            tmanPartners = selectView(newTmanPartners);
        }
    };
    
    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {

            // Convert the buffer from the strange format to an ArrayList<PeerAddress>
            ArrayList<PeerAddress> remoteAddresses = new ArrayList<PeerAddress>();
            for (PeerDescriptor descriptor : event.getSelectedBuffer().getDescriptors()) {
                remoteAddresses.add(descriptor.getPeerAddress());
            }

            // Merge our view and the received view and select the VIEW_SIZE highest ranked peers ar our new view
            List<PeerAddress> newTmanPartners = new ArrayList<PeerAddress>(tmanPartners);
            addUniqueToBufferOmitting(remoteAddresses, newTmanPartners, self);
            tmanPartners = selectView(newTmanPartners);
        }
    };

    /**
     * Receive notice from Search that it has detected a failed node and remove it from the view
     * TODO: Just a lame excuse to not implement real failure detection in TMan
     */
    Handler<TManKillNode> handleTManKillNode = new Handler<TManKillNode>() {
        @Override
        public void handle(TManKillNode message) {
            tmanPartners.remove(message.getNode());
        }
    };

}
