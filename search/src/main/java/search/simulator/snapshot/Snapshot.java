package search.simulator.snapshot;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.peer.PeerAddress;
import search.system.peer.search.Search;

public class Snapshot {
	private static HashMap<PeerAddress, PeerInfo> peers = new HashMap<PeerAddress, PeerInfo>();
	private static int counter = 0;
	private static String FILENAME = "aggregation.out";
    private static int lastLeaderCount = 0;
    private static float lastIndexDistPercentage = 0;
    private static int maxLeaderIndex = 0;
    private static int lastMaxLuceneIndex = 0;
    private static boolean allPeersJoined = false;
    private static int allPeersTotal = System.getenv("PEERS") != null ? Integer.parseInt(System.getenv("PEERS")) : 200;
    private static int allPeersJoinedTime;
    private static HashMap<String, String> reportedValues = new HashMap<String, String>();
    private static PeerAddress firstPeer = null;
    private static int indexAddMessages = 0;
    private static int indexPropagationMessages = 0;

    public static int getCounter() {
        return counter;
    }

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
	public static void init(int numOfStripes) {
		FileIO.write("", FILENAME);
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

//-------------------------------------------------------------------
	public static void updateCyclonPartners(PeerAddress address, ArrayList<PeerAddress> partners) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.updateCyclonPartners(partners);
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
    }

    public static void addIndexEntryCompleted() {
        if(isReported("addIndexEntryInitiated") && !isReported("addIndexEntryCompleted")) {
            reportValue("addIndexEntryTotalMessages", indexAddMessages);
            reportValue("addIndexEntryCompleted", getTicksSinceAllJoined() - getReportedValueAsInt("addIndexEntryInitiated"));
        }
        shutdownSimulation();
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
        boolean createReport = false;
        String o = "";

        if (!isReported("numberOfPeers")) {
            reportValue("numberOfPeers", allPeersTotal);
        }

        int numLeaders = getNumberOfLeaders();
        if (numLeaders == 1 && lastLeaderCount == 0 && !isReported("firstLeader")) {

            if (Snapshot.getLeaders().get(0).getPeerId().equals(BigInteger.ONE)) {
                reportValue("firstLeader", getTicksSinceAllJoined());
                //throw new RuntimeException("First leader is not correct leader! Expected 1, was " + Snapshot.getLeaders().get(0).getPeerId());
            }
        }

        if (numLeaders > lastLeaderCount && isReported("firstLeader") && isReported("originalLeaderDead") && !isReported("secondLeader")) {
            if (Snapshot.getLeaders().get(0).getPeerId().equals(new BigInteger("2"))) {
                reportValue("secondLeader", (getTicksSinceAllJoined() - (getReportedValueAsInt("originalLeaderDead") + getReportedValueAsInt("deadLeaderConfirmed"))));
                //throw new RuntimeException("Second leader is not correct leader! Expected 2, was " + Snapshot.getLeaders().get(0).getPeerId());
            }
        }

        if (false && numLeaders != lastLeaderCount) {
            lastLeaderCount = numLeaders;
            o += "# num leaders: " + numLeaders;
            for (PeerAddress leader : getLeaders()) {
                o += "(" + leader + ")";
            }
            o += "\n";
            createReport = true;
        }

        if (counter % 1 == 0) {
            float indexDistPercentage = getIndexDistPercentage();
            if (indexDistPercentage != lastIndexDistPercentage && (true || (int)indexDistPercentage == 100 || (int)indexDistPercentage == 0)) {
                if ((int)indexDistPercentage == 100 && isReported("indexPropagationStart") && !isReported("indexPropagationComplete")) {
                    Snapshot.indexEntryPropagationComplete();
                }
                lastIndexDistPercentage = indexDistPercentage;
                //o += "# index dist %: " + (int)indexDistPercentage + "\n";
                //createReport = true;
            }
        }

        if (lastMaxLuceneIndex != maxLeaderIndex) {
            lastMaxLuceneIndex = maxLeaderIndex;
            /*o += "# max index: " + maxLeaderIndex+ "\n";
            createReport = true;*/
        }

        if (createReport) {
            return o.trim();
        } else {
            return null;
        }
    }

    /*
    boolean newLowestPeer = false;
        for (PeerAddress peer : peersList) {
            if (lowestPeerID == null || peer.getPeerId().compareTo(lowestPeerID) == -1) {
                lowestPeerID = peer.getPeerId();
                newLowestPeer = true;
            }
        }

        if (newLowestPeer) {
            System.out.println("Lowest peer: " + lowestPeerID);
        }
     */

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

        //if (counter % 10000 == 1) {
            String report = createReport();
            if (report != null) System.out.println(report);
        //}
	}

//-------------------------------------------------------------------
	private static String reportNetworkState() {
		String str = new String("---\n");
		int totalNumOfPeers = peers.size();
		str += "total number of peers: " + totalNumOfPeers + "\n";

		return str;		
	}
	
//-------------------------------------------------------------------
	private static String reportDetailes() {
		PeerInfo peerInfo;
		String str = new String("---\n");

		for (PeerAddress peer : peers.keySet()) {
			peerInfo = peers.get(peer);
		
			str += "peer: " + peer;
			str += ", cyclon parters: " + peerInfo.getCyclonPartners();
			str += "\n";
		}
		
		return str;
	}

//-------------------------------------------------------------------
	private static String verifyNetworkSize() {
		PeerInfo peerInfo;
		int correct = 0;
		double estimated = 0;
		String str = new String("---\n");

		for (PeerAddress peer : peers.keySet()) {
			peerInfo = peers.get(peer);
			estimated = 1 / peerInfo.getNum();
			str += peer + " --> estimated size: " + estimated + "\n";
			if (Math.abs(estimated - peers.size()) <= peers.size() * 0.02)
				correct++;
		}
		
		str += "estimated correctly: " + correct + "\n";
		return str;
	}

}
