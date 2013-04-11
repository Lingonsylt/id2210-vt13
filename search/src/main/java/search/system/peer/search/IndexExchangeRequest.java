package search.system.peer.search;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

import java.math.BigInteger;


public class IndexExchangeRequest extends Message {
    int maxIndexID;
    BigInteger sourcePeerID;

    public IndexExchangeRequest(Address source, BigInteger sourcePeerID, Address destination, int maxIndexID) {
        super(source, destination);
        this.maxIndexID = maxIndexID;
        this.sourcePeerID = sourcePeerID;
    }

    public int getMaxIndexID() {
        return maxIndexID;
    }

    public BigInteger getSourcePeerID(){
        return sourcePeerID;
    }
}
