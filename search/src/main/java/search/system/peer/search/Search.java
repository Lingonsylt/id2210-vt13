package search.system.peer.search;

import org.apache.lucene.search.*;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.web.Web;
import se.sics.kompics.web.WebRequest;
import se.sics.kompics.web.WebResponse;
import tman.system.peer.tman.*;

/**
 * 
 * @author jdowling
 */
public final class Search extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    Random randomGenerator = new Random();

    // Ports
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanSamplePort = positive(TManSamplePort.class);

    // Peer
    private PeerAddress self;
    private Search that = this;

    /**
     * Indexing
     **/
    // The highest index in the local lucene database
    private int maxLuceneIndex = 0;

    // If leader, the next id that should be used when adding an entry to lucene
    private int nextId = 0;

    // Lucene setup
    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    Directory index = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // The peers closest to the top of the gradient, as discovered by cyclon. Used to route index add requests
    private ArrayList<PeerAddress> topmostCyclonPartners = new ArrayList<PeerAddress>();

    // The outstanding index add requests. Used for resending if timeout is reached
    private HashMap<UUID, LeaderRequestMessage> outstandingLeaderRequests = new HashMap<UUID, LeaderRequestMessage>();

    /**
     * Leader election
     **/
    private boolean isLeader = false;
    private boolean isRunningElection = false;
    private int electionYesVotes = 0;
    private int electionParticipants = 0;

    // If a part of the quorum, the current leader
    private PeerAddress leader = null;


    // The gradient neighbors
    ArrayList<PeerAddress> tmanPartners = new ArrayList<PeerAddress>();

    // The gradient neighbors last round, to be compared against current
    private ArrayList<PeerAddress> tmanPartnersLastRound = new ArrayList<PeerAddress>();

    // The number of rounds the gradient neighbors have been the same
    private int sameNeighborsRoundCount = 0;

    // The outstanding requests to peers in the elector group. Used to detect failure of leader and leader candidates
    private HashMap<UUID, PeerAddress> outstandingElectorHearbeats = new HashMap<UUID, PeerAddress>();

    // An up-to-date map describing the dead/alive state of the leader and leader candidates
    private HashMap<PeerAddress, Boolean> aliveElectors = new HashMap<PeerAddress, Boolean>();

    // Trying to wrap my head around how to encapsulate behaviour and split 1000 lines long files in Kompics
    private WebHandlers wh = new WebHandlers(new WebHandlersSearchConnector(), self, webPort, timerPort);

//-------------------------------------------------------------------	
    public Search() {

        subscribe(handleInit, control);
        subscribe(handleInspectTrigger, timerPort);

        subscribe(handleWebRequest, webPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManSample, tmanSamplePort);

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

    class WebHandlersSearchConnector {
        public <P extends PortType> void trigger(Event event, Port<P> port) {
            that.trigger(event, port);
        }

        public void addEntryAtClient(String key, String value) {
            that.addEntryAtClient(key, value);
        }

        public String query(String query) throws IOException, ParseException {
            return that.query(query);
        }
    }
//-------------------------------------------------------------------	
    Handler<SearchInit> handleInit = new Handler<SearchInit>() {
        public void handle(SearchInit init) {
            try {
                IndexWriter w = new IndexWriter(index, config);
                w.commit();
                w.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            self = init.getSelf();
            wh.setSelf(self);
            int num = init.getNum();
            SearchConfiguration searchConfiguration = init.getConfiguration();
            Snapshot.updateNum(self, num);
        }
    };

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * TODO: Remove machine specific paths in Snapshot.inspectOverlay
     * Draw the overlay using graphviz neato and display it using eye of gnome (requires graphviz and eye of gnome)
     */
    Handler<InspectTrigger> handleInspectTrigger = new Handler<InspectTrigger>() {
        public void handle(InspectTrigger trigger) {
            tman.simulator.snapshot.Snapshot.inspectOverlay(self.getPeerId());
        }
    };

    Handler<WebRequest> handleWebRequest = wh.handleWebRequest;



    private void addDocuments(List<Document> documents) throws IOException {
        IndexWriter w = new IndexWriter(index, config);
        for (Document doc : documents) {
            w.addDocument(doc);
            int docIndex = Integer.parseInt(doc.getField("index").stringValue());
            if (docIndex > maxLuceneIndex) {
                maxLuceneIndex = docIndex;
            }
        }
        w.close();
    }

    /*private void addEntry(String key, String value) throws IOException {

    }*/

    private void addEntryAtClient(String key, String value) {
        Snapshot.addIndexEntryInitiated();
        addEntryAtClient(key, value, null, UUID.randomUUID());
    }

    private void addEntryAtClient(String key, String value, PeerAddress relayFor, UUID requestID) {
        if(isLeader) {
            try {
                addEntryAtLeader(key, value);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
        } else {
            LeaderRequestMessage message = null;
            PeerAddress recipient = null; //leader;
            if(recipient == null) {
                recipient = getTopmostPartner();
            }
            PeerAddress sender = relayFor == null ? self : relayFor;

            if (recipient == null) {
                message = new LeaderRequestMessage(requestID, key, value, sender);
            } else {
                message = new LeaderRequestMessage(requestID, key, value, sender, recipient);
            }

            ScheduleTimeout rst = new ScheduleTimeout(10000);
            rst.setTimeoutEvent(new LeaderRequestMessageTimeout(rst, requestID, relayFor == null));
            trigger(rst, timerPort);
            outstandingLeaderRequests.put(requestID, message);

            if (recipient != null) {
                //System.out.println(self + ": Adding at: " + recipient);
                Snapshot.addIndexEntryMessageSent();
                trigger(message, networkPort);
            }
        }

    }

    Handler<LeaderHeartbeatTimeout> handleLeaderHeartbeatTimeout = new Handler<LeaderHeartbeatTimeout>() {
        public void handle(LeaderHeartbeatTimeout message) {
            if (outstandingElectorHearbeats.containsKey(message.getRequestID())) {
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    if (message.getElector().getPeerId().equals(BigInteger.ONE)) {
                        //System.out.println("One is down!");
                    } else {
                        //System.out.println(".");
                    }


                }

                if (message.getElector().equals(leader)) {
                    leader = null;
                    if (message.getElector().getPeerId().equals(new BigInteger("2"))) {
                        //System.out.println("Leader is dead!");
                    }
                    //System.out.println("Leader heartbeat timeout! Initiating leader election: " + self.getPeerId());
                    //System.out.println("Removing " + message.getElector().getPeerId() + ": " + tmanPartners.remove(message.getElector()));
                    initiateLeaderElection();
                }
                aliveElectors.put(message.getElector(), false);
                outstandingElectorHearbeats.remove(message.getRequestID());
                trigger(new TManKillNode(message.getElector()), tmanSamplePort);
            }
        }
    };

    Handler<LeaderRequestMessageTimeout> handleLeaderRequestMessageTimeout = new Handler<LeaderRequestMessageTimeout>() {
        public void handle(LeaderRequestMessageTimeout message) {
            LeaderRequestMessage outstanding = outstandingLeaderRequests.get(message.getRequestID());
            if (outstanding != null) {
                //System.out.println(self.getPeerId() + ": Request failed towards " + outstanding.getPeerDestination());
                topmostCyclonPartners.remove(outstanding.getPeerDestination());
                tmanPartners.remove(outstanding.getPeerDestination());
                if (message.getRetry()) {
                    if (tmanPartners.contains(outstanding.getPeerDestination()) || topmostCyclonPartners.contains(outstanding.getPeerDestination())) {
                        throw new RuntimeException("Remove failed!");
                    }
                    //System.out.println(self.getPeerId() + ": Retrying request " + outstanding.getRequestId());
                    addEntryAtClient(outstanding.getKey(), outstanding.getValue());
                }
            } else {
                //System.out.println("Succeeded with request " + message.getRequestID() + " at leader " + leader);
            }
        }
    };


    public int getMaxLuceneIndex() {
        return maxLuceneIndex;
    }

    private void addEntryAtLeader(String key, String value) throws IOException {
        maxLuceneIndex = ++nextId;
        //System.out.println("Item added: " + maxLuceneIndex + ". " + key + ", " + value);
        Snapshot.updateMaxLeaderIndex(maxLuceneIndex);
        Snapshot.addIndexEntryAtLeader();
        IndexWriter w = new IndexWriter(index, config);
        Document doc = new Document();
        doc.add(new TextField("title", key, Field.Store.YES));
        // You may need to make the StringField searchable by NumericRangeQuery. See:
        // http://stackoverflow.com/questions/13958431/lucene-4-0-indexwriter-updatedocument-for-numeric-term
        // http://lucene.apache.org/core/4_2_0/core/org/apache/lucene/document/IntField.html
        doc.add(new StringField("id", value, Field.Store.YES));
        doc.add(new StringField("index", String.format("%05d", maxLuceneIndex), Field.Store.YES));
        w.addDocument(doc);
        w.close();
    }

    private List<Document> getDocumentsSinceIndex(int sinceIndex) {
        List<Document> documents = new LinkedList<Document>();

        String queryString = "index:[" + String.format("%05d", sinceIndex) + " TO 99999]";
        Query q = null;
        try {
            q = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(queryString);
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        IndexSearcher searcher = null;
        IndexReader reader = null;
        TopDocs results = null;
        ScoreDoc[] hits = null;
        try {
            reader = DirectoryReader.open(index);
            searcher = new IndexSearcher(reader);
            results = searcher.search(q, 65536);
            hits = results.scoreDocs;

            for(int i=0;i<hits.length;++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                documents.add(d);
            }
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        return documents;
    }

    private String query(String queryString) throws ParseException, IOException {
        // the "title" arg specifies the default field to use when no field is explicitly specified in the query.
        Query q = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(queryString);
        IndexSearcher searcher = null;
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(index);
            searcher = new IndexSearcher(reader);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        StringBuilder sb = new StringBuilder();
        // display results
        sb.append("<div>Found ").append(hits.length).append(" entries.</div>");
        sb.append("<table><tr><td>index</td><td>title</td><td>value</td>");
        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            sb.append("<tr><td>").append(d.get("index")).append("</td><td>").append(d.get("title")).append("</td><td>").append(d.get("id")).append("</td></tr>");
        }
        sb.append("</table>");

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
        return sb.toString();
    }

    private void addTopMostCyclonPartners(List<PeerAddress> parnters) {
        for(PeerAddress peer : parnters) {
            if (topmostCyclonPartners.contains(peer)) {
                continue;
            }
            if (topmostCyclonPartners.size() < 10) {
                topmostCyclonPartners.add(peer);
            } else {
                PeerAddress bottom = getBottomCyclonPartner();
                if(peer.compareTo(bottom) == -1) {
                    topmostCyclonPartners.remove(bottom);
                    topmostCyclonPartners.add(peer);
                }
            }
        }
    }

    private PeerAddress getBottomCyclonPartner() {
        PeerAddress bottom = null;
        for(PeerAddress peer : topmostCyclonPartners) {
            if(bottom == null) {
                bottom = peer;
            } else {
                if (peer.compareTo(bottom) == 1) {
                    bottom = peer;
                }
            }
        }
        return bottom;
    }

    private PeerAddress getTopmostPartner() {
        PeerAddress top = null;
        for(PeerAddress peer : topmostCyclonPartners) {
            if(top == null) {
                top = peer;
            } else {
                if (peer.compareTo(top) == -1) {
                    top = peer;
                }
            }
        }

        for(PeerAddress peer : tmanPartners) {
            if(top == null) {
                top = peer;
            } else {
                if (peer.compareTo(top) == -1) {
                    top = peer;
                }
            }
        }

        if (top == null || self.compareTo(top) <= 0) {
            return null;
        }
        return top;
    }

    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {

            Snapshot.updateSearch(self, that);
            Snapshot.report(self);
            if (!Snapshot.hasAllPeersJoined()) {
                return;
            }
            //tman.simulator.snapshot.Snapshot.report();
            // receive a new list of neighbours
            ArrayList<PeerAddress> sampleNodes = event.getSample();

            if (sampleNodes.size() > 0) {
                addTopMostCyclonPartners(sampleNodes);
            }

            // Pick a node or more, and exchange index with them
        }
    };

    void updateAliveElectorsStatus() {
        for(PeerAddress peer : tmanPartners) {
            if(!aliveElectors.containsKey(peer)) {
                aliveElectors.put(peer, true);
            }
        }
    }

    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            if (!Snapshot.hasAllPeersJoined()) {
                return;
            }

            // receive a new list of neighbours
            tmanPartners = event.getSample();

            if (tmanPartners.size() > 1) {
                Snapshot.addIndexPropagationMessageSent();
                trigger(new IndexExchangeRequest(self.getPeerAddress(), self.getPeerId(), tmanPartners.get(randomGenerator.nextInt(tmanPartners.size())).getPeerAddress(), maxLuceneIndex), networkPort);
            }

            checkForLeadership();
            heartbeatElectors();
            updateAliveElectorsStatus();
            tmanPartnersLastRound = new ArrayList<PeerAddress>(tmanPartners);
        }
    };

    Handler<IndexExchangeRequest> handleIndexExchangeRequest = new Handler<IndexExchangeRequest>() {
        @Override
        public void handle(IndexExchangeRequest event) {
            if (event.getMaxIndexID() < maxLuceneIndex) {
                Snapshot.addIndexPropagationMessageSent();
                trigger(new IndexExchangeResponse(self.getPeerAddress(), self.getPeerId(), event.getSource(), getDocumentsSinceIndex(event.getMaxIndexID())), networkPort);
            }
        }
    };

    String documentListToString(List<Document> documents) {
        String output = "Documents(";
        for (Document doc : documents) {
            output += doc.getField("index").stringValue();
        }
        output += ")";
        return output;
    }

    Handler<IndexExchangeResponse> handleIndexExchangeResponse = new Handler<IndexExchangeResponse>() {
        @Override
        public void handle(IndexExchangeResponse event) {
            //System.out.println(self.toString() + " <== " + event.getSourcePeerID() + ": " + documentListToString(event.documents));
            try {
                addDocuments(event.documents);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    };

    public void initiateLeaderElection() {
        if (!isLeader && !isRunningElection) {
            Snapshot.leaderElectionStarted();
            //System.out.println(self + ": leader election started!");
            isRunningElection = true;
            electionYesVotes = 0;
            electionParticipants = tmanPartners.size();

            for(PeerAddress neighbor : tmanPartners) {
                trigger(new LeaderElectionMessage(UUID.randomUUID(), "AM_I_LEGEND", self, neighbor), networkPort);
            }
        }
    }

    public boolean isLowestPeer(PeerAddress peer) {
        return isLowestPeer(peer, tmanPartners);
    }

    public boolean isLowestPeer(PeerAddress peer, List<PeerAddress> partners) {
        for(PeerAddress neighbor : partners) {
            if (aliveElectors.containsKey(neighbor) && aliveElectors.get(neighbor) && neighbor.getPeerId().compareTo(peer.getPeerId()) == -1) {
                return false;
            }
        }
        return true;
    }

    public void heartbeatElectors() {
        if (leader != null && !isLeader) {
            if (self.getPeerId().equals(new BigInteger("2"))) {
                //System.out.println("Hearbeating nr 2!");
            }

            List<PeerAddress> electors = new ArrayList<PeerAddress>(tmanPartners);
            if (!electors.contains(leader)) {
                electors.add(leader);
            }

            for (PeerAddress elector : tmanPartners) {
                if (!outstandingElectorHearbeats.values().contains(elector)) {
                    ScheduleTimeout rst = new ScheduleTimeout(1000);
                    LeaderHeartbeatTimeout timeoutMessage = new LeaderHeartbeatTimeout(rst, elector);
                    rst.setTimeoutEvent(timeoutMessage);
                    outstandingElectorHearbeats.put(timeoutMessage.getRequestID(), elector);
                    //leaderHeartbeatOutstanding = timeoutMessage.getRequestID();
                    trigger(rst, timerPort);
                    trigger(new LeaderElectionMessage(timeoutMessage.getRequestID(), "ARE_YOU_ALIVE", self, elector), networkPort);
                }
            }
        }
    }

    public void checkForLeadership() {
        if(isRunningElection) {
            return;
        }

        if (isLeader) {
            if (!isLowestPeer(self)) {
                isLeader = false;
            }
        } else {
            if (leader != null && !isLowestPeer(leader)) {
                trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, leader), networkPort);
            }
        }

        if (tmanPartnersLastRound.equals(tmanPartners)) {

            sameNeighborsRoundCount++;
        } else {
            sameNeighborsRoundCount = 0;
        }
        if (self.getPeerId().equals(new BigInteger("2"))) {
            //System.out.println("2: sameNeighborsRoundCount: " + sameNeighborsRoundCount);
        }

        if (tmanPartners.size() == TMan.C && sameNeighborsRoundCount >= 3) {
            if (isLowestPeer(self)) {
                initiateLeaderElection();
            } else {
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    //System.out.println("2: not lowest peer: " + tmanPartners + ", " + outstandingElectorHearbeats + " ### " + aliveElectors);
                }
            }
        }
    }

    public void announceLeadership() {
        isLeader = true;
        isRunningElection = false;

        for(PeerAddress neighbor : tmanPartners) {
            //System.out.println("Announce: " + neighbor);
            trigger(new LeaderElectionMessage(UUID.randomUUID(), "I_AM_LEGEND", self, neighbor), networkPort);
        }
    }
    private String prettyPrintPeerAddressesList(List<PeerAddress> addresses) {
        String output = "";
        for (PeerAddress t : addresses) {
            output += t.toString() + ", ";
        }
        return output;
    }

    Handler<SimulationAddIndexEntry> handleSimulationAddIndexEntry = new Handler<SimulationAddIndexEntry>() {
        @Override
        public void handle(SimulationAddIndexEntry message) {
            //System.out.println(self.getPeerId() + ": Adding index entry: " + message.getKey() + ", " + message.getValue());
            addEntryAtClient(message.getKey(), message.getValue());
        }
    };

    Handler<LeaderElectionMessage> handleLeaderElectionIncoming = new Handler<LeaderElectionMessage>() {
        @Override
        public void handle(LeaderElectionMessage message) {
            if (message.getCommand().equals("AM_I_LEGEND")) {
                if (isLowestPeer(message.getPeerSource())) {
                    trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LEGEND", maxLuceneIndex, self, message.getPeerSource()), networkPort);
                } else {
                    //if (self.getPeerId().equals(new BigInteger("2")) && message.getPeerSource().getPeerId().equals(new BigInteger("1"))) {
                    if (message.getPeerSource().getPeerId().equals(new BigInteger("2"))) {
                        //System.out.println(self.getPeerId() + " says " + message.getPeerSource().getPeerId()  + " is not lowest: " + prettyPrintPeerAddressesList(tmanPartners));
                    }
                    trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, message.getPeerSource()), networkPort);
                }
            } else if (message.getCommand().equals("YOU_ARE_LEGEND")) {
                if (isRunningElection) {
                    electionYesVotes++;
                    if (message.getNextId() > nextId) {
                        nextId = message.getNextId();
                    }
                    if (electionYesVotes > electionParticipants / 2) {
                        announceLeadership();
                    }
                }
            } else if (message.getCommand().equals("I_AM_LEGEND")) {

                leader = message.getPeerSource();
                if (self.getPeerId().equals(new BigInteger("2"))) {
                    //System.out.println("New leader!" + leader);
                }
            } else if (message.getCommand().equals("YOU_ARE_LOSER")) {
                if (isLeader) {
                    //System.out.println("I AM LOSER! " + self.getPeerId());
                    isLeader = false;
                }
                isRunningElection = false;
            } else if (message.getCommand().equals("ARE_YOU_ALIVE")) {
                if (self.getPeerId().equals(new BigInteger("1"))) {
                    //System.out.println("1: alive");
                }
                trigger(new LeaderElectionMessage(message.getRequestId(), "I_AM_ALIVE", self, message.getPeerSource()), networkPort);
            } else if (message.getCommand().equals("I_AM_ALIVE")) {
                if(outstandingElectorHearbeats.get(message.getRequestId()) != null) {
                    outstandingElectorHearbeats.remove(message.getRequestId());
                    aliveElectors.put(message.getPeerSource(), true);
                }
            }
        }
    };



    Handler<LeaderRequestMessage> handleLeaderRequestMessage = new Handler<LeaderRequestMessage>() {
        @Override
        public void handle(LeaderRequestMessage request) {
            if (isLeader) {
                try {
                    addEntryAtLeader(request.getKey(), request.getValue());
                    //System.out.println(self.getPeerId() + ": add response to " + request.getPeerSource());
                    Snapshot.addIndexEntryMessageSent();
                    trigger(new LeaderResponseMessage(request.getRequestId(), self, request.getPeerSource()), networkPort);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            } else {
                addEntryAtClient(request.getKey(), request.getValue(), request.getPeerSource(), request.getRequestId());
            }
        }
    };

    Handler<LeaderResponseMessage> handleLeaderResponseMessage = new Handler<LeaderResponseMessage>() {
        @Override
        public void handle(LeaderResponseMessage response) {
            //System.out.println(self.getPeerId() + ": Successfully added an entry at leader!");
            outstandingLeaderRequests.remove(response.getRequestId());
            Snapshot.addIndexEntryCompleted();
        }
    };
}
