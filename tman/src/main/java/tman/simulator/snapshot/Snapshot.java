package tman.simulator.snapshot;

import common.peer.PeerAddress;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;


public class Snapshot {
	private static TreeMap<PeerAddress, PeerInfo> peers = new TreeMap<PeerAddress, PeerInfo>();
	private static int counter = 0;
	private static String FILENAME = "tman.out";
    private static BigInteger lowestPeerID = null;

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

//-------------------------------------------------------------------
	public static void updateTManPartners(PeerAddress address, ArrayList<PeerAddress> partners) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.updateTManPartners(partners);
	}
	
//-------------------------------------------------------------------
	public static void updateCyclonPartners(PeerAddress address, ArrayList<PeerAddress> partners) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.updateCyclonPartners(partners);
	}

    public static void inspectOverlay(BigInteger highlightPeer) {
        PeerAddress[] peersList = new PeerAddress[peers.size()];
        peers.keySet().toArray(peersList);

        Snapshot.printDotFile(peersList, highlightPeer);
        //System.out.println(str);
        Process p = null;
        try {

            p = Runtime.getRuntime().exec("neato -Tpng /home/lingon/dev/dsearch/graph.dot -Goverlap=false -o /home/lingon/dev/dsearch/graph.png");
            p.waitFor();
            p = Runtime.getRuntime().exec("eog /home/lingon/dev/dsearch/graph.png");
            p.waitFor();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
//-------------------------------------------------------------------
	public static void report() {

		PeerAddress[] peersList = new PeerAddress[peers.size()];
		peers.keySet().toArray(peersList);
        counter++;

        if (counter == -3500) {
            System.out.println("Number of peers: " + peers.size());

            System.exit(1);
            Snapshot.inspectOverlay(null);
            System.exit(1);
        }
		//System.out.println(str);
		//FileIO.append(str, FILENAME);


    }

    public static void printDotFile(PeerAddress[] peersList, BigInteger highlightPeer) {
        String output = "";
        output += "digraph tman {\n";
        output += "graph [ dpi = 300 ]; \n";
        for (PeerAddress peerAddress : peersList) {
            output += "\n    // start " + peerAddress.getPeerId() + "\n";
            PeerInfo peerInfo = peers.get(peerAddress);

            if (peerAddress.getPeerId().equals(highlightPeer)) {
                output += "    " + peerAddress.getPeerId() + " [style=bold, color=blue];";

            }

            for (PeerAddress neighborAddress : peerInfo.getTManPartners()) {
                output += "    " + peerAddress.getPeerId() + " -> " + neighborAddress.getPeerId() + ";\n";
            }
        }

        output += "}\n";

        //System.out.println(output);
        FileIO.write(output, "/home/lingon/dev/dsearch/graph.dot");
    }

//-------------------------------------------------------------------
	private static String reportNetworkState() {
		String str = new String("---\n");
		int totalNumOfPeers = peers.size() - 1;
		str += "total number of peers: " + totalNumOfPeers + "\n";

		return str;		
	}
	
//-------------------------------------------------------------------
	private static String reportDetails() {
		PeerInfo peerInfo;
		String str = new String("---\n");

		for (PeerAddress peer : peers.keySet()) {

			peerInfo = peers.get(peer);

			str += "peer: " + peer;
			str += ", cyclon parters: " + peerInfo.getCyclonPartners();
			str += ", tman parters (" + peerInfo.getTManPartners().size() + "): " + peerInfo.getTManPartners();
			str += "\n";
		}
		
		return str;
	}
	

}
