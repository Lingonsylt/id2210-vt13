package tman.simulator.snapshot;

import common.peer.PeerAddress;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;


public class Snapshot {
	private static TreeMap<PeerAddress, PeerInfo> peers = new TreeMap<PeerAddress, PeerInfo>();
    private static int allPeersTotal = System.getenv("PEERS") != null ? Integer.parseInt(System.getenv("PEERS")) : 200;
    private static boolean allPeersJoined = false;

//-------------------------------------------------------------------
	public static void addPeer(PeerAddress address) {
		peers.put(address, new PeerInfo());
        if (!allPeersJoined) {
            if (peers.size() == allPeersTotal) {
                allPeersJoined = true;
            }
        }
	}

    public static boolean hasAllPeersJoined() {
        return allPeersJoined;
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

    /**
     * Create a .dot-file containing the overlay, draw a png of it with 'neato', and display it with 'eog'
     */
    public static void inspectOverlay(BigInteger highlightPeer) {
        PeerAddress[] peersList = new PeerAddress[peers.size()];
        peers.keySet().toArray(peersList);

        Snapshot.printDotFile(peersList, highlightPeer);
        Process p = null;
        try {

            p = Runtime.getRuntime().exec("neato -Tpng /home/lingon/dev/dsearch/graph.dot -Goverlap=false -o /home/lingon/dev/dsearch/graph.png");
            p.waitFor();
            p = Runtime.getRuntime().exec("eog /home/lingon/dev/dsearch/graph.png");
            p.waitFor();
        } catch (IOException e) {
            e.printStackTrace();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

        // TODO: Fix the local machine specific paths
        FileIO.write(output, "/home/lingon/dev/dsearch/graph.dot");
    }
}
