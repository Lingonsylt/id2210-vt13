package search.system.peer.search;

import org.apache.lucene.search.*;
import search.simulator.snapshot.Snapshot;
import common.configuration.SearchConfiguration;
import common.peer.PeerAddress;
import cyclon.system.peer.cyclon.CyclonSample;
import cyclon.system.peer.cyclon.CyclonSamplePort;
import java.io.IOException;
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
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);
    Negative<Web> webPort = negative(Web.class);
    Positive<CyclonSamplePort> cyclonSamplePort = positive(CyclonSamplePort.class);
    Positive<TManSamplePort> tmanSamplePort = positive(TManSamplePort.class);
    Random randomGenerator = new Random();
    private int maxLuceneIndex = 0;
    private PeerAddress self;
    private long period;
    private double num;
    private SearchConfiguration searchConfiguration;
    // Apache Lucene used for searching
    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    Directory index = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    private HashMap<UUID, LeaderRequestMessage> outstandingLeaderRequests = new HashMap<UUID, LeaderRequestMessage>();
    private boolean isLeader = false;
    private int nextId = 0;
    private int sameNeighborsRoundCount = 0;
    ArrayList<PeerAddress> tmanPartners = new ArrayList<PeerAddress>();
    private ArrayList<PeerAddress> tmanPartnersLastRound = new ArrayList<PeerAddress>();
    private ArrayList<PeerAddress> topmostCyclonPartners = new ArrayList<PeerAddress>();
    private boolean isRunningElection = false;
    private int electionYesVotes = 0;
    private int electionParticipants = 0;
    private PeerAddress leader = null;

//-------------------------------------------------------------------	
    public Search() {

        subscribe(handleInit, control);
        subscribe(handleUpdateIndex, timerPort);
        subscribe(handleWebRequest, webPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManSample, tmanSamplePort);

        subscribe(handleLeaderElectionIncoming, networkPort);

        subscribe(handleLeaderRequestMessage, networkPort);
        subscribe(handleLeaderResponseMessage, networkPort);

        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);

        subscribe(handleLeaderRequestMessageTimeout, timerPort);


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
            num = init.getNum();
            searchConfiguration = init.getConfiguration();
            period = searchConfiguration.getPeriod();

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateIndexTimeout(rst));
            trigger(rst, timerPort);

            Snapshot.updateNum(self, num);
            //addEntryAtClient("The Art of Computer Science", "100", true);

        }
    };
//-------------------------------------------------------------------	
    Handler<UpdateIndexTimeout> handleUpdateIndex = new Handler<UpdateIndexTimeout>() {
        public void handle(UpdateIndexTimeout event) {
        }
    };
    Handler<WebRequest> handleWebRequest = new Handler<WebRequest>() {
        public void handle(WebRequest event) {
            final String SEARCH_COMMAND = "search", ADD_COMMAND = "add";
            List<String> allowedCommands = Arrays.asList(SEARCH_COMMAND, ADD_COMMAND);

            if (event.getDestination() != self.getPeerAddress().getId()) {
                return;
            }
            logger.debug("Handling Webpage Request");

            org.mortbay.jetty.Request jettyRequest = event.getRequest();
            String pathInfoString = jettyRequest.getPathInfo();
            String command = "";
            if (pathInfoString != null && !pathInfoString.equals("")) {
                String[] pathInfos = event.getRequest().getPathInfo().split("/");
                if (pathInfos.length != 0) {
                    command = pathInfos[pathInfos.length - 1];
                }
            }
            WebResponse response;
            if (!allowedCommands.contains(command)) {
                response = WebHelpers.createBadRequestResponse(event, "Invalid command!: " + command);
            } else {
                if (command.equals(SEARCH_COMMAND)) {
                    String queryString = WebHelpers.getParamOrDefault(jettyRequest, "query", null);
                    if (queryString != null) {
                        String queryResult = null;
                        try {
                            queryResult = query(queryString);
                        } catch (IOException e) {
                            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
                        } catch (ParseException e) {
                            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, e);
                            e.printStackTrace();
                        }
                        if (queryResult != null) {
                            response = WebHelpers.createDefaultRenderedResponse(event, "Search succeded!", queryResult);
                        } else {
                            response = WebHelpers.createErrorResponse(event, "Failure searching for " + queryString + "!<br />");
                        }
                    } else {
                        response = WebHelpers.createBadRequestResponse(event, "Invalid query value");
                    }

                } else if (command.equals(ADD_COMMAND)) {
                    String key = WebHelpers.getParamOrDefault(jettyRequest, "key", null);
                    String value = WebHelpers.getParamOrDefault(jettyRequest, "value", null);
                    if (key != null && value != null) {
                        IOException addException = null;
                        addEntryAtClient(key, value, true);
                        if (addException == null) {
                            response = WebHelpers.createDefaultRenderedResponse(event, "Uploaded item into network!", "Added " + key + " with value " + value + "!");
                        } else {
                            response = WebHelpers.createErrorResponse(event, "Failure adding " + key + " with value " + value + "!<br />" + addException.getMessage());
                        }
                    } else {
                        response = WebHelpers.createBadRequestResponse(event, "Invalid key or value");
                    }
                } else {
                    response = WebHelpers.createBadRequestResponse(event, "Invalid command");
                }
            }
            trigger(response, webPort);
        }
    };

    static class WebHelpers {
        public static WebResponse createBadRequestResponse(WebRequest event, String message) {
            return new WebResponse(createBadRequestHtml(message), event, 1, 1);
        }

        public static String getParamOrDefault(org.mortbay.jetty.Request jettyRequest, String param, String defaultValue) {
            return (jettyRequest.getParameter(param) == null ||
                    jettyRequest.getParameter(param).equals("")) ?
                    defaultValue : jettyRequest.getParameter(param);
        }

        public static WebResponse createErrorResponse(WebRequest event, String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", "Error!");
            params.put("message", message);
            return new WebResponse(renderHtmlTemplate(getDefaultHtmlTemplate(), params), event, 1, 1);
        }

        public static WebResponse createDefaultRenderedResponse(WebRequest event, String title, String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", title);
            params.put("message", message);
            return new WebResponse(renderHtmlTemplate(getDefaultHtmlTemplate(), params), event, 1, 1);
        }

        public static String getDefaultHtmlTemplate() {
            StringBuilder sb = new StringBuilder("<!DOCTYPE html PUBLIC \"-//W3C");
            sb.append("//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR");
            sb.append("/xhtml1/DTD/xhtml1-transitional.dtd\"><html xmlns=\"http:");
            sb.append("//www.w3.org/1999/xhtml\"><head><meta http-equiv=\"Conten");
            sb.append("t-Type\" content=\"text/html; charset=utf-8\" />");
            sb.append("<title>Adding an Entry</title>");
            sb.append("<style type=\"text/css\"><!--.style2 {font-family: ");
            sb.append("Arial, Helvetica, sans-serif; color: #0099FF;}--></style>");
            sb.append("</head><body><h2 align=\"center\" class=\"style2\">");
            sb.append("ID2210 {{ title }}</h2><br>{{ message }}</body></html>");
            return sb.toString();
        }

        public static String renderHtmlTemplate(String html, Map<String, String> params) {
            for (String key : params.keySet()) {
                html = html.replaceAll("\\{\\{[\\s]?" + key + "[\\s]?}}", params.get(key));
            }
            return html;
        }

        public static String createBadRequestHtml(String message) {
            Map<String, String> params = new HashMap<String, String>();
            params.put("title", "Bad request!");
            params.put("message", message);
            return renderHtmlTemplate(getDefaultHtmlTemplate(), params);
        }
    }

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

    private void addEntryAtClient(String key, String value, boolean retry) {
        if(isLeader) {
            try {
                addEntryAtLeader(key, value);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
        } else {
            UUID requestID = UUID.randomUUID();

            LeaderRequestMessage message = null;
            PeerAddress recipient = leader;
            if(recipient == null) {
                recipient = getTopmostPartner();
            }
            if (recipient == null) {
                message = new LeaderRequestMessage(requestID, key, value, self);
            } else {
                message = new LeaderRequestMessage(requestID, key, value, self, recipient);
            }

            if (retry) {
                SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(10000, 10000);
                rst.setTimeoutEvent(new LeaderRequestMessageTimeout(rst, requestID));
                trigger(rst, timerPort);
                outstandingLeaderRequests.put(requestID, message);
            }

            if (recipient != null) {
                System.out.println("Sending add " + key + ", " + message.getRequestId() + " to " + message.getPeerDestination().getPeerId());
                trigger(message, networkPort);
            }
        }

    }

    Handler<LeaderRequestMessageTimeout> handleLeaderRequestMessageTimeout = new Handler<LeaderRequestMessageTimeout>() {
        public void handle(LeaderRequestMessageTimeout message) {
            LeaderRequestMessage outstanding = outstandingLeaderRequests.get(message.getRequestID());
            if (outstanding != null) {
                //System.out.println("Retrying request " + outstanding.getRequestId() + " at leader " + leader);
                addEntryAtClient(outstanding.getKey(), outstanding.getValue(), true);
            } else {
                //System.out.println("Succeeded with request " + message.getRequestID() + " at leader " + leader);
            }
        }
    };


    private void addEntryAtLeader(String key, String value) throws IOException {
        maxLuceneIndex = ++nextId;
        System.out.println("Leader max id: " + maxLuceneIndex);
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
        System.out.println("Added entry: " + maxLuceneIndex + ", " + key + ", " + value);
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
        sb.append("<table><tr>Found ").append(hits.length).append(" entries.</tr>");
        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            sb.append("<tr>").append(i + 1).append(". ").append(d.get("id")).append("\t").append(d.get("title")).append("</tr>");
        }
        sb.append("</table>");

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
        return sb.toString();
    }

    private void addTopMostCyclonPartners(List<PeerAddress> parnters) {
        for(PeerAddress peer : parnters) {
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
            tman.simulator.snapshot.Snapshot.report();
            // receive a new list of neighbours
            ArrayList<PeerAddress> sampleNodes = event.getSample();

            if (sampleNodes.size() > 0) {
                addTopMostCyclonPartners(sampleNodes);
            }

            // Pick a node or more, and exchange index with them
        }
    };
    
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {


            // receive a new list of neighbours
            tmanPartners = event.getSample();

            if (tmanPartners.size() > 1) {
                trigger(new IndexExchangeRequest(self.getPeerAddress(), self.getPeerId(), tmanPartners.get(randomGenerator.nextInt(tmanPartners.size())).getPeerAddress(), maxLuceneIndex), networkPort);
            }
            checkForLeadership();
            tmanPartnersLastRound = new ArrayList<PeerAddress>(tmanPartners);
        }
    };

    Handler<IndexExchangeRequest> handleIndexExchangeRequest = new Handler<IndexExchangeRequest>() {
        @Override
        public void handle(IndexExchangeRequest event) {
            if (isLeader) {
                System.out.println("Leader request for: " + event.getMaxIndexID());
            }
            if (event.getMaxIndexID() < maxLuceneIndex) {
                System.out.println(event.getSourcePeerID() + "Request for >" + event.getMaxIndexID() + " served with " + maxLuceneIndex + ", " + self.getPeerId());
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
        isRunningElection = true;
        electionYesVotes = 0;
        electionParticipants = tmanPartners.size();

        for(PeerAddress neighbor : tmanPartners) {
            trigger(new LeaderElectionMessage(UUID.randomUUID(), "AM_I_LEGEND", self, neighbor), networkPort);
        }
    }

    public boolean isLowestPeer(PeerAddress peer) {
        return isLowestPeer(peer, tmanPartners);
    }

    public boolean isLowestPeer(PeerAddress peer, List<PeerAddress> partners) {
        for(PeerAddress neighbor : partners) {
            if (neighbor.getPeerId().compareTo(peer.getPeerId()) == -1) {
                return false;
            }
        }
        return true;
    }

    public void checkForLeadership() {
        if(isRunningElection) {
            return;
        }

        if (isLeader) {
            if (!isLowestPeer(self)) {
                isLeader = false;
                System.out.println("I AM LOSER! " + self.getPeerId());
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

        if (tmanPartners.size() == TMan.C && sameNeighborsRoundCount == 3) {
            if (isLowestPeer(self)) {
                initiateLeaderElection();
            }
        }
    }

    public void announceLeadership() {
        isLeader = true;
        isRunningElection = false;
        System.out.println("I AM LEGEND: " + self.getPeerId());
        for(PeerAddress neighbor : tmanPartners) {
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

    Handler<LeaderElectionMessage> handleLeaderElectionIncoming = new Handler<LeaderElectionMessage>() {
        @Override
        public void handle(LeaderElectionMessage message) {
            if (message.getCommand().equals("AM_I_LEGEND")) {
                if (isLowestPeer(message.getPeerSource())) {
                    trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LEGEND", nextId, self, message.getPeerSource()), networkPort);
                } else {
                    trigger(new LeaderElectionMessage(UUID.randomUUID(), "YOU_ARE_LOSER", self, message.getPeerSource()), networkPort);
                }
            } else if (message.getCommand().equals("YOU_ARE_LEGEND")) {
                if (isRunningElection) {
                    electionYesVotes++;
                    if (electionYesVotes > electionParticipants / 2) {
                        announceLeadership();
                    }
                }
            } else if (message.getCommand().equals("I_AM_LEGEND")) {
                leader = message.getPeerSource();
            } else if (message.getCommand().equals("YOU_ARE_LOSER")) {
                if (isLeader) {
                    System.out.println("I AM LOSER! " + self.getPeerId());
                    isLeader = false;
                }
                isRunningElection = false;
            }
        }
    };

    Handler<LeaderRequestMessage> handleLeaderRequestMessage = new Handler<LeaderRequestMessage>() {
        @Override
        public void handle(LeaderRequestMessage request) {
            if (isLeader) {
                try {
                    addEntryAtLeader(request.getKey(), request.getValue());
                    trigger(new LeaderResponseMessage(request.getRequestId(), self, request.getPeerSource()), networkPort);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            } else {
                System.out.println("Relaying add " + request.getKey()+ ", " + request.getRequestId() + " to " + request.getPeerDestination().getPeerId());
                addEntryAtClient(request.getKey(), request.getValue(), false);
            }
        }
    };

    Handler<LeaderResponseMessage> handleLeaderResponseMessage = new Handler<LeaderResponseMessage>() {
        @Override
        public void handle(LeaderResponseMessage response) {
            //System.out.println("Successfully added an entry at leader!");
            outstandingLeaderRequests.remove(response.getRequestId());
        }
    };
}
