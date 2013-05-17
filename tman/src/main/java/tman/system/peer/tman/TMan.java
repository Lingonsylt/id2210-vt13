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

    Negative<TManSamplePort> tmanPartnersPort = negative(TManSamplePort.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Random randomGenerator;
    private HashMap<UUID, PeerAddress> outstandingRequests;
    private CyclonConfiguration cyclonConfiguration;
    private long period;
    private PeerAddress self;
    private ArrayList<PeerAddress> tmanPartners;
    private TManConfiguration tmanConfiguration;
    private ArrayList<PeerAddress> lastSeenCyclonPartners;
    public static final int C = 6;



    public class TManSchedule extends Timeout {

        public TManSchedule(SchedulePeriodicTimeout request) {
            super(request);
        }

//-------------------------------------------------------------------
        public TManSchedule(ScheduleTimeout request) {
            super(request);
        }
    }

//-------------------------------------------------------------------	
    public TMan() {
        tmanPartners = new ArrayList<PeerAddress>();
        outstandingRequests = new HashMap<UUID, PeerAddress>();
        randomGenerator = new Random();
        lastSeenCyclonPartners = new ArrayList<PeerAddress>();

        subscribe(handleInit, control);
        subscribe(handleRound, timerPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManPartnersResponse, networkPort);
        subscribe(handleTManPartnersRequest, networkPort);
    }

    void prettyPrintPeerAddressesList(List<PeerAddress> addresses) {
        String output = "";
        for (PeerAddress t : addresses) {
            output += t.toString() + ", ";
        }
        System.out.println(output);
    }
//-------------------------------------------------------------------	
    Handler<TManInit> handleInit = new Handler<TManInit>() {
        @Override
        public void handle(TManInit init) {
            self = init.getSelf();
            tmanConfiguration = init.getConfiguration();
            cyclonConfiguration = init.getCyclonConfiguration();
            period = tmanConfiguration.getPeriod();
            Snapshot.addPeer(self);

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new TManSchedule(rst));
            trigger(rst, timerPort);

            /*self = new PeerAddress(null, BigInteger.valueOf(5));
            List<PeerAddress> pa = new ArrayList<PeerAddress>();

            pa.add(new PeerAddress(null, BigInteger.valueOf(1)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(2)));
            //pa.add(new PeerAddress(null, BigInteger.valueOf(3)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(4)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(5)));
            //pa.add(new PeerAddress(null, BigInteger.valueOf(6)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(7)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(8)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(9)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(10)));
            pa.add(new PeerAddress(null, BigInteger.valueOf(11)));


            Collections.sort(pa, rankingComparator);
            prettyPrintPeerAddressesList(selectView(pa));

            for (int i = 0; i < 100; i++) {
                System.out.println(selectPeer(pa));
            }


            prettyPrintPeerAddressesList(pa);
            System.exit(0);*/
        }
    };
//-------------------------------------------------------------------	
    Handler<TManSchedule> handleRound = new Handler<TManSchedule>() {
        @Override
        public void handle(TManSchedule event) {
            if (tmanPartners.size() > C) {
                System.out.println("HR!");
                System.exit(1);
            }
            Snapshot.updateTManPartners(self, tmanPartners);
            Snapshot.updateCyclonPartners(self, lastSeenCyclonPartners);

            // Publish sample to connected components
            trigger(new TManSample(tmanPartners), tmanPartnersPort);            
        }
    };

    /*Comparator<PeerAddress> rankingComparator = new Comparator<PeerAddress>() {
        public int compare(PeerAddress left, PeerAddress right) {
            BigInteger rankLeft = rankingFunction(left.getPeerId());
            BigInteger rankRight = rankingFunction(right.getPeerId());
            if (rankLeft == null && rankRight == null) {
                return 0;
            } else if (rankLeft == null) {
                return 1;
            } else if (rankRight == null) {
                return -1;
            } else {
                return rankLeft.compareTo(rankRight);
            }
        }};*/

    Comparator<PeerAddress> rankingComparator = new Comparator<PeerAddress>() {
        public int compare(PeerAddress left, PeerAddress right) {
            if (left.getPeerId() == null && right.getPeerId() == null) {
                return 0;
            } else if (left.getPeerId() == null) {
                return -1;
            } else if (right.getPeerId() == null) {
                return 1;
            } else {
                if (left.getPeerId().compareTo(self.getPeerId()) > 0 && right.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return left.compareTo(right);

                } else if (left.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return 1;

                } else if (right.getPeerId().compareTo(self.getPeerId()) > 0) {
                    return -1;

                } else {
                    return right.compareTo(left);
                }
            }
        }};

    BigInteger rankingFunction(BigInteger item) {
        if (item.equals(self.getPeerId())) {
            throw new RuntimeException("Cannot compare to self!");
        }

        //if (item.compareTo(self.getPeerId()) < 0) {
        //    return null;
        //} else {
        return item.subtract(self.getPeerId());
        //}
    }

    void addUniqueToBufferOmitting(List<PeerAddress> add, List<PeerAddress> buffer, PeerAddress... omitting) {
        addUniqueToBufferOmitting(add, buffer, false, omitting);
    }

    void addUniqueToBufferOmitting(List<PeerAddress> add, List<PeerAddress> buffer, boolean keepRoof, PeerAddress... omitting) {
        for (PeerAddress peerAddress : add) {
            if (keepRoof && buffer.size() == C) {
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

    void addUniqueToBufferOmitting(PeerAddress add, List<PeerAddress> buffer, PeerAddress... omitting) {
        if (!buffer.contains(add)) {
            boolean omit = false;
            for (PeerAddress omitPeer : omitting) {
                if (add.equals(omitPeer)) {
                    omit = true;
                    break;
                }
            }
            if (!omit) {
                buffer.add(add);
            }
        }
    }

    void addToBufferTillMax(List<PeerAddress> add, List<PeerAddress> buffer) {
        for (PeerAddress peerAddress : add) {
            if (buffer.size() == C) {
                break;
            }
            if (!buffer.contains(peerAddress)) {
                buffer.add(peerAddress);
            }
        }
    }

    PeerAddress selectPeer(List<PeerAddress> view) {
        List<PeerAddress> sortCopy = new ArrayList<PeerAddress>(view);
        Collections.sort(sortCopy, rankingComparator);
        int halfBufferSize = sortCopy.size() / 2;
        int randomPeerIndex = randomGenerator.nextInt(halfBufferSize + 1);
        return sortCopy.get(randomPeerIndex);
    }

    ArrayList<PeerAddress> selectView(List<PeerAddress> view) {
        List<PeerAddress> sortCopy = new ArrayList<PeerAddress>(view);
        Collections.sort(sortCopy, rankingComparator);

        ArrayList<PeerAddress> result = new ArrayList<PeerAddress>();
        int stopAt = Math.min(C, view.size());
        for (int i = 0; i < stopAt; i++) {
            result.add(sortCopy.get(i));
        }
        return result;

    }







//-------------------------------------------------------------------
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            ArrayList<PeerAddress> cyclonPartners = event.getSample();

            if (tmanPartners.size() != 0) {

                lastSeenCyclonPartners = new ArrayList<PeerAddress>(cyclonPartners);


                /*Collections.sort(buffer, rankingComparator);

                int halfBufferSize = buffer.size() / 2;
                int randomPeerIndex = randomGenerator.nextInt(halfBufferSize + 1);

                PeerAddress receivingPeer = buffer.get(randomPeerIndex);*/
                PeerAddress receivingPeer = selectPeer(tmanPartners);
                List<PeerAddress> buffer = new ArrayList<PeerAddress>(tmanPartners);
                buffer.remove(receivingPeer);
                buffer.add(self);

                addUniqueToBufferOmitting(cyclonPartners, buffer, receivingPeer);

                ScheduleTimeout rst = new ScheduleTimeout(cyclonConfiguration.getShuffleTimeout());
                rst.setTimeoutEvent(new ShuffleTimeout(rst, receivingPeer));
                UUID rTimeoutId = rst.getTimeoutEvent().getTimeoutId();

                outstandingRequests.put(rTimeoutId, receivingPeer);

                ArrayList<PeerDescriptor> bufferDescriptors = new ArrayList<PeerDescriptor>();
                for (PeerAddress peer : buffer) {
                    bufferDescriptors.add(new PeerDescriptor(peer));
                }
                DescriptorBuffer descriptorBuffer = new DescriptorBuffer(self, bufferDescriptors);
                trigger(new ExchangeMsg.Request(rTimeoutId, descriptorBuffer, self, receivingPeer), networkPort);
            } else {
                addUniqueToBufferOmitting(cyclonPartners, tmanPartners, true, self);
                if (tmanPartners.size() > C) {
                    System.out.println("CY!");
                    System.exit(1);
                }
                /*for (PeerAddress peerAddress : cyclonPartners) {
                    if (!peerAddress.getPeerId().equals(self.getPeerId())) {
                        tmanPartners.add(peerAddress);
                    }
                }*/
            }
        }
    };

    String peerAddressListToString(List<PeerAddress> addresses) {
        String output = "";
        for (PeerAddress address : addresses) {
            output += address.getPeerId() + ", ";
        }
        return output;
    }
//-------------------------------------------------------------------	
    Handler<ExchangeMsg.Request> handleTManPartnersRequest = new Handler<ExchangeMsg.Request>() {
        @Override
        public void handle(ExchangeMsg.Request event) {
            ArrayList<PeerAddress> cyclonPartners = new ArrayList<PeerAddress>(lastSeenCyclonPartners);

            List<PeerAddress> buffer = new ArrayList<PeerAddress>(tmanPartners);
            buffer.add(self);

            addUniqueToBufferOmitting(cyclonPartners, buffer, event.getPeerSource());

            ArrayList<PeerDescriptor> bufferDescriptors = new ArrayList<PeerDescriptor>();
            for (PeerAddress peer : buffer) {
                bufferDescriptors.add(new PeerDescriptor(peer));
            }
            DescriptorBuffer descriptorBuffer = new DescriptorBuffer(self, bufferDescriptors);
            trigger(new ExchangeMsg.Response(event.getRequestId(), descriptorBuffer, self, event.getPeerSource()), networkPort);

            //addUniqueToBufferOmitting(cyclonPartners, buffer, event.getPeerSource());

            ArrayList<PeerAddress> remoteAddresses = new ArrayList<PeerAddress>();
            for (PeerDescriptor descriptor : event.getRandomBuffer().getDescriptors()) {
                remoteAddresses.add(descriptor.getPeerAddress());
            }


            List<PeerAddress> newTmanPartners = new ArrayList<PeerAddress>(tmanPartners);
            //addUniqueToBufferOmitting(, newTmanPartners, self);
            addUniqueToBufferOmitting(remoteAddresses, newTmanPartners, self);
            tmanPartners = selectView(newTmanPartners);

            if (tmanPartners.size() > C) {
                System.out.println("RQ!");
                System.exit(1);
            }
        }
    };
    
    Handler<ExchangeMsg.Response> handleTManPartnersResponse = new Handler<ExchangeMsg.Response>() {
        @Override
        public void handle(ExchangeMsg.Response event) {


            ArrayList<PeerAddress> remoteAddresses = new ArrayList<PeerAddress>();
            for (PeerDescriptor descriptor : event.getSelectedBuffer().getDescriptors()) {
                remoteAddresses.add(descriptor.getPeerAddress());
            }

            List<PeerAddress> newTmanPartners = new ArrayList<PeerAddress>(tmanPartners);
            //addUniqueToBufferOmitting(, newTmanPartners, self);
            addUniqueToBufferOmitting(remoteAddresses, newTmanPartners, self);
            tmanPartners = selectView(newTmanPartners);

            if (tmanPartners.size() > C) {
                System.out.println("RS!");
                System.exit(1);
            }
        }
    };

}
