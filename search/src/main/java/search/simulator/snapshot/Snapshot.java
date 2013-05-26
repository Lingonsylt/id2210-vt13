package search.simulator.snapshot;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.peer.PeerAddress;
import search.system.peer.search.Search;

public class Snapshot {
	private static HashMap<PeerAddress, PeerInfo> peers = new HashMap<PeerAddress, PeerInfo>();

    private static String FILENAME = "aggregation.out";

    // Gossip round count
    private static int counter = 0;

    // The number of leaders last round
    private static int lastLeaderCount = 0;

    // The percent of index distribution last round
    private static float lastIndexDistPercentage = 0;

    // The highest lucene index of the leader
    private static int maxLeaderIndex = 0;

    // The highest lucene index of the leader last round
    private static int lastMaxLuceneIndex = 0;

    // True if all peers in Simulation1 has joined
    private static boolean allPeersJoined = false;

    // The total number of peers in Simulation1 that will join
    private static int allPeersTotal = System.getenv("PEERS") != null ? Integer.parseInt(System.getenv("PEERS")) : 200;

    // The time in gossip rounds when all peers joined
    private static int allPeersJoinedTime;

    // All reported statistics
    private static HashMap<String, String> reportedValues = new HashMap<String, String>();

    // The first peer to join. Used to measure gossip rounds in a hacky way
    private static PeerAddress firstPeer = null;

    // Number of messages sent concerning the add of a new entry
    private static int indexAddMessages = 0;

    // Number of messages send concerning dissemination of index entries
    private static int indexPropagationMessages = 0;

    public static boolean hasAllPeersJoined() {
        return allPeersJoined;
    }

    public static int getTicksSinceAllJoined() {
        if (!allPeersJoined) {
            throw new RuntimeException("All peers haven't joined yet!");
        }
        return counter - allPeersJoinedTime;
    }

//-------------------------------------------------------------------
	public static void addPeer(PeerAddress address) {
        if(firstPeer == null) {
            firstPeer = address;
        }
		peers.put(address, new PeerInfo());
        if (!allPeersJoined) {
            if (peers.size() == allPeersTotal) {
                reportValue("allPeersJoined", counter);
                allPeersJoined = true;
                allPeersJoinedTime = counter;
            }
        }
	}

//-------------------------------------------------------------------
	public static void removePeer(PeerAddress address) {
        if (address.getPeerId().equals(BigInteger.ONE)) {
            reportValue("originalLeaderDead", getTicksSinceAllJoined());
        }
		peers.remove(address);
	}

    public static void updateMaxLeaderIndex(int index) {
        maxLeaderIndex = index;
    }

//-------------------------------------------------------------------
	public static void updateNum(PeerAddress address, double num) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.updateNum(num);
	}

    public static void updateSearch(PeerAddress address, Search search) {
        PeerInfo peerInfo = peers.get(address);

        if (peerInfo == null)
            return;

        peerInfo.updateSearch(search);
    }

    public static void addIndexEntryInitiated() {
        if(!isReported("addIndexEntryInitiated")) {
            indexAddMessages = 0;
            if (!isReported("secondLeader")) {
                throw new RuntimeException("Second leader has to be present before index measurements start!");
            }
            reportValue("addIndexEntryInitiated", getTicksSinceAllJoined());
        }
    }

    public static void addIndexEntryAtLeader() {
        indexPropagationMessages = 0;
        reportValue("indexPropagationStart", getTicksSinceAllJoined());
    }

    private static void indexEntryPropagationComplete() {
        reportValue("indexPropagationComplete", getTicksSinceAllJoined() - getReportedValueAsInt("indexPropagationStart"));
        reportValue("indexPropagationTotalMessages", indexPropagationMessages);
        shutdownSimulation();
    }

    public static void addIndexEntryCompleted() {
        if(isReported("addIndexEntryInitiated") && !isReported("addIndexEntryCompleted")) {
            reportValue("addIndexEntryTotalMessages", indexAddMessages);
            reportValue("addIndexEntryCompleted", getTicksSinceAllJoined() - getReportedValueAsInt("addIndexEntryInitiated"));
        }

    }

    public static void shutdownSimulation() {
        System.exit(0);
    }

    public static void addIndexEntryMessageSent() {
        indexAddMessages++;
    }

    public static void addIndexPropagationMessageSent() {
        indexPropagationMessages++;
    }

    public static float getIndexDistPercentage() {
        PeerAddress[] peersList = new PeerAddress[peers.size()];
        peers.keySet().toArray(peersList);

        int numPeers = 0;
        int numWithFullIndex = 0;
        for (PeerAddress peer : peersList) {
            PeerInfo peerInfo = peers.get(peer);
            if (peerInfo.getSearch() != null) {
                numPeers++;
                if (peerInfo.getSearch().getMaxLuceneIndex() > maxLeaderIndex) {
                    throw new RuntimeException("Node with index higher than leader found!: " + peerInfo);
                }
                if (peerInfo.getSearch().getMaxLuceneIndex() == maxLeaderIndex) {
                    numWithFullIndex += 1;
                }
            }
        }

        return ((float)numWithFullIndex / (float)numPeers) * 100;
    }

    public static void reportValue(String key, int value) {
        reportValue(key, value + "");
    }

    public static void reportValue(String key, String value) {
        reportedValues.put(key, value);
        System.out.println("" + key + "\t" + value);
    }

    public static int getReportedValueAsInt(String key) {
        return Integer.parseInt(getReportedValue(key));
    }

    public static String getReportedValue(String key) {
        if (!reportedValues.containsKey(key)) {
            throw new RuntimeException("Key " + key + " has not been reported!");
        }

        return reportedValues.get(key);
    }


    public static boolean isReported(String key) {
        return reportedValues.containsKey(key);
    }


    public static void leaderElectionStarted() {
        if (isReported("firstLeader") && !isReported("secondLeader") && !isReported("deadLeaderConfirmed")) {
            reportValue("deadLeaderConfirmed", getTicksSinceAllJoined() - getReportedValueAsInt("originalLeaderDead"));
        }
    }

    public static String createReport() {
        if (!isReported("numberOfPeers")) {
            reportValue("numberOfPeers", allPeersTotal);
        }

        int numLeaders = getNumberOfLeaders();
        if (numLeaders == 1 && lastLeaderCount == 0 && !isReported("firstLeader")) {

            if (Snapshot.getLeaders().get(0).getPeerId().equals(BigInteger.ONE)) {
                reportValue("firstLeader", getTicksSinceAllJoined());
            }
        }

        if (numLeaders > lastLeaderCount && isReported("firstLeader") && isReported("originalLeaderDead") && !isReported("secondLeader")) {
            if (Snapshot.getLeaders().get(0).getPeerId().equals(new BigInteger("2"))) {
                reportValue("secondLeader", (getTicksSinceAllJoined() - (getReportedValueAsInt("originalLeaderDead") + getReportedValueAsInt("deadLeaderConfirmed"))));
            }
        }

        float indexDistPercentage = getIndexDistPercentage();
        if (indexDistPercentage != lastIndexDistPercentage) {
            if ((int)indexDistPercentage == 100 && isReported("indexPropagationStart") && !isReported("indexPropagationComplete")) {
                Snapshot.indexEntryPropagationComplete();
            }
            lastIndexDistPercentage = indexDistPercentage;
        }

        if (lastMaxLuceneIndex != maxLeaderIndex) {
            lastMaxLuceneIndex = maxLeaderIndex;
        }

        return null;
    }

    public static int getNumberOfLeaders() {
        return getLeaders().size();
    }

    private static List<PeerAddress> getLeaders() {
        PeerAddress[] peersList = new PeerAddress[peers.size()];
        peers.keySet().toArray(peersList);

        List<PeerAddress> leaders = new ArrayList<PeerAddress>();
        for (PeerAddress peer : peersList) {
            PeerInfo peerInfo = peers.get(peer);
            if (peerInfo.getSearch() != null && peerInfo.getSearch().isLeader()) {
                leaders.add(peer);
            }
        }
        return leaders;
    }


//-------------------------------------------------------------------
	public static void report(PeerAddress peer) {
        if (peer != firstPeer) {
            return;
        }
        counter++;

        String report = createReport();
        if (report != null) System.out.println(report);
	}
}
