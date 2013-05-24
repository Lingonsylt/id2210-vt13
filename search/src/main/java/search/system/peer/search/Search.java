package search.system.peer.search;

import se.sics.kompics.timer.ScheduleTimeout;
import search.simulator.core.SimulationAddIndexEntry;
import search.simulator.snapshot.Snapshot;
import se.sics.kompics.Port;
import se.sics.kompics.PortType;
import se.sics.kompics.Event;
import common.configuration.SearchConfiguration;
import common.peer.PeerAddress;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.logging.Level;

import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import tman.system.peer.tman.*;

/**
 * 
 * @author jdowling
 */
public final class Search extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Search.class);


    // Ports
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanSamplePort = positive(TManSamplePort.class);

    // Peer
    private PeerAddress self;
    private Search that = this;


    private IndexingService indexingService;
    private IndexExchangeService indexExchangeService;

    private IndexNextIdService indexNextIdService;
    private LeaderElectionService leaderElectionService;
    private IndexAddService indexAddService;
    private WebService webService;

    Handler<InspectTrigger> handleInspectTrigger;
    Handler<WebRequest> handleWebRequest;

    Handler<IndexExchangeRequest> handleIndexExchangeRequest;
    Handler<IndexExchangeResponse> handleIndexExchangeResponse;

    Handler<LeaderElectionMessage> handleLeaderElectionIncoming;
    Handler<LeaderHeartbeatTimeout> handleLeaderHeartbeatTimeout;

    Handler<LeaderRequestMessageTimeout> handleLeaderRequestMessageTimeout;
    Handler<LeaderRequestMessage> handleLeaderRequestMessage;
    Handler<LeaderResponseMessage> handleLeaderResponseMessage;
//-------------------------------------------------------------------	
    public Search() {

        subscribe(handleInit, control);



        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManSample, tmanSamplePort);



    }

    public void setUpServices(PeerAddress self) {
        // Trying to wrap my head around how to encapsulate behaviour and split 1000 lines long files in Kompics
        indexingService = new IndexingService(self, timerPort);
        indexExchangeService = new IndexExchangeService(new TriggerDependency(), indexingService, self, networkPort);

        indexNextIdService = new IndexNextIdService();
        leaderElectionService = new LeaderElectionService(new TriggerDependency(), indexingService, indexNextIdService, self, tmanSamplePort, networkPort, timerPort);
        indexAddService = new IndexAddService(new TriggerDependency(), leaderElectionService, indexingService, indexNextIdService, self, networkPort, timerPort);
        webService = new WebService(new TriggerDependency(), indexAddService, indexingService, self, webPort, timerPort);

        handleInspectTrigger = webService.handleInspectTrigger;
        handleWebRequest = webService.handleWebRequest;

        handleIndexExchangeRequest = indexExchangeService.handleIndexExchangeRequest;
        handleIndexExchangeResponse = indexExchangeService.handleIndexExchangeResponse;

        handleLeaderElectionIncoming = leaderElectionService.handleLeaderElectionIncoming;
        handleLeaderHeartbeatTimeout = leaderElectionService.handleLeaderHeartbeatTimeout;

        handleLeaderRequestMessageTimeout = indexAddService.handleLeaderRequestMessageTimeout;
        handleLeaderRequestMessage = indexAddService.handleLeaderRequestMessage;
        handleLeaderResponseMessage = indexAddService.handleLeaderResponseMessage;


        subscribe(handleWebRequest, webPort);
        subscribe(handleInspectTrigger, timerPort);

        // Handle leader election and heartbeat
        subscribe(handleLeaderElectionIncoming, networkPort);
        subscribe(handleLeaderHeartbeatTimeout, timerPort);

        // Add entry to the index
        subscribe(handleLeaderRequestMessage, networkPort);
        subscribe(handleLeaderResponseMessage, networkPort);
        subscribe(handleLeaderRequestMessageTimeout, timerPort);

        // Exchange index entries
        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);

        // Receive AddIndexEntry messages from the Scenarios,
        subscribe(handleSimulationAddIndexEntry, networkPort);


    }

    class TriggerDependency {
        public <P extends PortType> void trigger(Event event, Port<P> port) {
            that.trigger(event, port);
        }
    }

//-------------------------------------------------------------------	
    Handler<SearchInit> handleInit = new Handler<SearchInit>() {
        public void handle(SearchInit init) {

            self = init.getSelf();
            setUpServices(self);
            int num = init.getNum();
            Snapshot.updateNum(self, num);
        }
    };

    /**
     * Handlers for web requests
     */


    public int getMaxLuceneIndex() {
        return indexingService.getMaxLuceneIndex();
    }

    public boolean isLeader() {
        return leaderElectionService.isLeader();
    }

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



    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            if (!Snapshot.hasAllPeersJoined()) {
                return;
            }
            leaderElectionService.receiveTManSample(event.getSample());
        }
    };

    Handler<SimulationAddIndexEntry> handleSimulationAddIndexEntry = new Handler<SimulationAddIndexEntry>() {
        @Override
        public void handle(SimulationAddIndexEntry message) {
            //System.out.println(self.getPeerId() + ": Adding index entry: " + message.getKey() + ", " + message.getValue());
            indexAddService.addEntryAtClient(message.getKey(), message.getValue());
        }
    };






}
