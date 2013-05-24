package search.system.peer.search;

import common.peer.PeerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import search.simulator.snapshot.Snapshot;

import java.io.IOException;

public class IndexExchangeService {
    private static final Logger logger = LoggerFactory.getLogger(IndexExchangeService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Network> networkPort;
    Search.TriggerDependency triggerDependency;
    IndexingService indexingService;

    public IndexExchangeService(Search.TriggerDependency triggerDependency, IndexingService indexingService, PeerAddress self, Positive<Network> networkPort) {
        this.triggerDependency = triggerDependency;
        this.self = self;
        this.networkPort = networkPort;
        this.indexingService = indexingService;
    }

    public void setSelf(PeerAddress self) {
        this.self = self;
    }

    public Handler<IndexExchangeRequest> handleIndexExchangeRequest = new Handler<IndexExchangeRequest>() {
        @Override
        public void handle(IndexExchangeRequest event) {
            if (event.getMaxIndexID() < indexingService.getMaxLuceneIndex()) {
                Snapshot.addIndexPropagationMessageSent();
                triggerDependency.trigger(new IndexExchangeResponse(self.getPeerAddress(), self.getPeerId(), event.getSource(), indexingService.getDocumentsSinceIndex(event.getMaxIndexID())), networkPort);
            }
        }
    };

    public Handler<IndexExchangeResponse> handleIndexExchangeResponse = new Handler<IndexExchangeResponse>() {
        @Override
        public void handle(IndexExchangeResponse event) {
            //System.out.println(self.toString() + " <== " + event.getSourcePeerID() + ": " + documentListToString(event.documents));
            try {
                indexingService.addDocuments(event.documents);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    };
}
