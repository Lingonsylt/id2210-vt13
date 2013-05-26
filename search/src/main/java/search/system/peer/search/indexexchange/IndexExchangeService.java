package search.system.peer.search.indexexchange;

import common.peer.PeerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import search.simulator.snapshot.Snapshot;
import search.system.peer.search.indexing.IndexingService;
import search.system.peer.search.Search;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Exchange index entries with gradient neighbors
 */
public class IndexExchangeService {
    private static final Logger logger = LoggerFactory.getLogger(IndexExchangeService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Network> networkPort;
    Search.TriggerDependency triggerDependency;
    IndexingService indexingService;

    Random randomGenerator = new Random();

    public IndexExchangeService(Search.TriggerDependency triggerDependency, IndexingService indexingService, PeerAddress self, Positive<Network> networkPort) {
        this.triggerDependency = triggerDependency;
        this.self = self;
        this.networkPort = networkPort;
        this.indexingService = indexingService;
    }

    /**
     * Receive a TMan sample from the Search-layer
     * Request to exchange index entries with someone higher than us in the gradient (random peer among TMan partners)
     */
    public void receiveTManSample(List<PeerAddress> tmanSample) {
        if (tmanSample.size() > 1) {
            Snapshot.addIndexPropagationMessageSent();
            triggerDependency.trigger(new IndexExchangeRequest(self.getPeerAddress(), self.getPeerId(), tmanSample.get(randomGenerator.nextInt(tmanSample.size())).getPeerAddress(), indexingService.getMaxLuceneIndex()), networkPort);
        }
    }

    /**
     * Respond with the index entries that are higher than the requesting nodes max, if any
     * TODO: Doesn't respond if there's nothing to give. Not good for failure detection
     */
    public Handler<IndexExchangeRequest> handleIndexExchangeRequest = new Handler<IndexExchangeRequest>() {
        @Override
        public void handle(IndexExchangeRequest event) {
            if (event.getMaxIndexID() < indexingService.getMaxLuceneIndex()) {
                Snapshot.addIndexPropagationMessageSent();
                triggerDependency.trigger(new IndexExchangeResponse(self.getPeerAddress(), self.getPeerId(), event.getSource(), indexingService.getDocumentsSinceIndex(event.getMaxIndexID())), networkPort);
            }
        }
    };

    /**
     * Add the received lucene documents to our local index
     */
    public Handler<IndexExchangeResponse> handleIndexExchangeResponse = new Handler<IndexExchangeResponse>() {
        @Override
        public void handle(IndexExchangeResponse event) {
            try {
                indexingService.addDocuments(event.documents);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    };
}
