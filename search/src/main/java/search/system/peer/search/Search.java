package search.system.peer.search;

import search.simulator.core.SimulationAddIndexEntry;
import search.simulator.snapshot.Snapshot;
import se.sics.kompics.Port;
import se.sics.kompics.PortType;
import se.sics.kompics.Event;
import common.peer.PeerAddress;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import search.system.peer.search.indexadd.IndexAddService;
import search.system.peer.search.indexexchange.IndexExchangeService;
import search.system.peer.search.indexing.IndexingService;
import search.system.peer.search.indexnextid.IndexNextIdService;
import search.system.peer.search.leaderelection.LeaderElectionService;
import search.system.peer.search.web.WebService;
import tman.system.peer.tman.*;

public final class Search extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Search.class);

    // Ports
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanSamplePort = positive(TManSamplePort.class);

    // Myself
    private PeerAddress self;
    private Search that = this;

    // Services (set up in Search.setUpServices())
    private IndexingService indexingService;
    private IndexExchangeService indexExchangeService;
    private IndexNextIdService indexNextIdService;
    private LeaderElectionService leaderElectionService;
    private IndexAddService indexAddService;
    private WebService webService;

//-------------------------------------------------------------------	
    public Search() {
        // Subscribe to the control channels. Application specific subscriptions happen in setUpServices()
        subscribe(handleInit, control);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManSample, tmanSamplePort);

        // Receive SimulationAddIndexEntry messages originally from the Scenarios
        subscribe(handleSimulationAddIndexEntry, networkPort);
    }

    /**
     * Set up services for the indexing application and define their dependencies
     * Also set up subscriptions for all services
     *
     * (Trying to wrap my head around how to encapsulate behaviour and split 1000 lines long files in Kompics)
     */
    public void setUpServices(PeerAddress self) {
        // Indexing: Adding and deleting from the local lucene index
        indexingService = new IndexingService(self, timerPort);

        // Index exchange: Exchange index entries between peers
        indexExchangeService = new IndexExchangeService(new TriggerDependency(), indexingService, self, networkPort);
        subscribe(indexExchangeService.handleIndexExchangeRequest, networkPort);
        subscribe(indexExchangeService.handleIndexExchangeResponse, networkPort);

        // Index next id: Keep track of the highest next id in the swarm. Only used by leader
        indexNextIdService = new IndexNextIdService();

        // Leader election: Keep track of who is leader
        leaderElectionService = new LeaderElectionService(new TriggerDependency(), indexingService, indexNextIdService, self, tmanSamplePort, networkPort, timerPort);
        subscribe(leaderElectionService.handleLeaderElectionIncoming, networkPort);
        subscribe(leaderElectionService.handleLeaderHeartbeatTimeout, timerPort);

        // Index add: Add an index to the swarm, from any client
        indexAddService = new IndexAddService(new TriggerDependency(), leaderElectionService, indexingService, indexNextIdService, self, networkPort, timerPort);
        subscribe(indexAddService.handleLeaderRequestMessage, networkPort);
        subscribe(indexAddService.handleLeaderResponseMessage, networkPort);
        subscribe(indexAddService.handleLeaderRequestMessageTimeout, timerPort);

        // Web: Handle add, search and inspect requests through HTTP
        webService = new WebService(new TriggerDependency(), indexAddService, indexingService, self, webPort, timerPort);
        subscribe(webService.handleWebRequest, webPort);
        subscribe(webService.handleInspectTrigger, timerPort);
    }

    /**
     * Wrapper around dependency for Search.trigger().
     * Used to expose .trigger() to services without breaking contract
     */
    public class TriggerDependency {
        public <P extends PortType> void trigger(Event event, Port<P> port) {
            // Relay call to Search().trigger()
            that.trigger(event, port);
        }
    }

//-------------------------------------------------------------------	
    Handler<SearchInit> handleInit = new Handler<SearchInit>() {
        public void handle(SearchInit init) {
            self = init.getSelf();

            // Set upp all application services
            setUpServices(self);

            Snapshot.updateNum(self, init.getNum());
        }
    };


    /**
     * Receive Cyclon samples and relay them to the IndexAddService
     * The Snapshot.report()-heartbeat is triggered from here
     */
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            Snapshot.updateSearch(self, that);
            Snapshot.report(self);
            if (!Snapshot.hasAllPeersJoined()) {
                return;
            }

            ArrayList<PeerAddress> sampleNodes = event.getSample();

            if (sampleNodes.size() > 0) {
                indexAddService.receiveCyclonSample(sampleNodes);
            }
        }
    };

    /**
     * Receive TMan samples and relay them to the LeaderElectionService
     */
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            if (!Snapshot.hasAllPeersJoined()) {
                return;
            }
            leaderElectionService.receiveTManSample(event.getSample());
            indexExchangeService.receiveTManSample(event.getSample());
        }
    };

    /**
     * Handle commands sent by Scenario1 through Operations.addIndexEntry
     * I couldn't figure out a good way to do this in Kompics
     */
    Handler<SimulationAddIndexEntry> handleSimulationAddIndexEntry = new Handler<SimulationAddIndexEntry>() {
        @Override
        public void handle(SimulationAddIndexEntry message) {
            indexAddService.addEntryAtClient(message.getKey(), message.getValue());
        }
    };

    /**
     * Reporting methods used by Snapshot. Not for application use
     */
    public int getMaxLuceneIndex() {
        return indexingService.getMaxLuceneIndex();
    }

    public boolean isLeader() {
        return leaderElectionService.isLeader();
    }
}
