package search.simulator.snapshot;

import java.util.ArrayList;
import java.util.HashMap;

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

//-------------------------------------------------------------------
	public static void init(int numOfStripes) {
		FileIO.write("", FILENAME);
	}

//-------------------------------------------------------------------
	public static void addPeer(PeerAddress address) {
		peers.put(address, new PeerInfo());
	}

//-------------------------------------------------------------------
	public static void removePeer(PeerAddress address) {
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
                    throw new RuntimeException("Node with index higher than leader foun!: " + peerInfo);
                }
                if (peerInfo.getSearch().getMaxLuceneIndex() == maxLeaderIndex) {
                    numWithFullIndex += 1;
                }
            }
        }

        return ((float)numWithFullIndex / (float)numPeers) * 100;
    }

    public static String createReport() {
        boolean createReport = false;
        String o = "";
        int numLeaders = getNumberOfLeaders();
        if (numLeaders != lastLeaderCount) {
            lastLeaderCount = numLeaders;
            o += "# num leaders: " + numLeaders + "\n";
            createReport = true;
        }

        if (counter % 1 == 0) {
            float indexDistPercentage = getIndexDistPercentage();
            if (Math.abs((int)indexDistPercentage - (int)lastIndexDistPercentage) >= 10) {
                lastIndexDistPercentage = indexDistPercentage;
                o += "# index dist %: " + (int)indexDistPercentage + "\n";
                createReport = true;
            }
        }

        if (lastMaxLuceneIndex != maxLeaderIndex) {
            lastMaxLuceneIndex = maxLeaderIndex;
            o += "# max index: " + maxLeaderIndex+ "\n";
            createReport = true;
        }
        o += "#=#=#=#=#=#";
        if (createReport) {
            return o;
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
        PeerAddress[] peersList = new PeerAddress[peers.size()];
        peers.keySet().toArray(peersList);

        int numLeaders = 0;
        for (PeerAddress peer : peersList) {
            PeerInfo peerInfo = peers.get(peer);
            if (peerInfo.getSearch() != null && peerInfo.getSearch().isLeader()) {
                numLeaders += 1;
            }
        }

        return numLeaders;
    }


//-------------------------------------------------------------------
	public static void report() {
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
