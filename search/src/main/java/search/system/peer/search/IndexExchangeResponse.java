package search.system.peer.search;

import org.apache.lucene.document.Document;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

import java.math.BigInteger;
import java.util.List;


public class IndexExchangeResponse extends Message {
    List<Document> documents;
    BigInteger sourcePeerID;

    public IndexExchangeResponse(Address source, BigInteger sourcePeerID, Address destination, List<Document> documents) {
        super(source, destination);
        this.documents = documents;
        this.sourcePeerID = sourcePeerID;
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public BigInteger getSourcePeerID(){
        return sourcePeerID;
    }
}