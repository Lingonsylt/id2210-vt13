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
import tman.system.peer.tman.TManSample;
import tman.system.peer.tman.TManSamplePort;

/**
 * 
 * @author jdowling
 */
public final class Search extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Search.class);
    private static int luceneIndexCounter = 0;
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

//-------------------------------------------------------------------	
    public Search() {

        subscribe(handleInit, control);
        subscribe(handleUpdateIndex, timerPort);
        subscribe(handleWebRequest, webPort);
        subscribe(handleCyclonSample, cyclonSamplePort);
        subscribe(handleTManSample, tmanSamplePort);

        subscribe(handleIndexExchangeRequest, networkPort);
        subscribe(handleIndexExchangeResponse, networkPort);
    }
//-------------------------------------------------------------------	
    Handler<SearchInit> handleInit = new Handler<SearchInit>() {
        public void handle(SearchInit init) {
            self = init.getSelf();
            num = init.getNum();
            searchConfiguration = init.getConfiguration();
            period = searchConfiguration.getPeriod();

            SchedulePeriodicTimeout rst = new SchedulePeriodicTimeout(period, period);
            rst.setTimeoutEvent(new UpdateIndexTimeout(rst));
            trigger(rst, timerPort);

            Snapshot.updateNum(self, num);
            try {
                addEntry("The Art of Computer Science", "100");
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
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
                        try {
                            addEntry(key, value);
                        } catch (IOException ex) {
                            addException = ex;
                            java.util.logging.Logger.getLogger(Search.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        if (addException == null) {
                            response = WebHelpers.createDefaultRenderedResponse(event, "Uploaded item into network!", "Added " + key + " with value" + value + "!");
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

    private void addEntry(String key, String value) throws IOException {
        maxLuceneIndex = luceneIndexCounter++;

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
    
    Handler<CyclonSample> handleCyclonSample = new Handler<CyclonSample>() {
        @Override
        public void handle(CyclonSample event) {
            // receive a new list of neighbours
            ArrayList<PeerAddress> sampleNodes = event.getSample();

            if (sampleNodes.size() > 0) {
                trigger(new IndexExchangeRequest(self.getPeerAddress(), self.getPeerId(), sampleNodes.get(randomGenerator.nextInt(sampleNodes.size())).getPeerAddress(), maxLuceneIndex), networkPort);
            }

            // Pick a node or more, and exchange index with them
        }
    };
    
    Handler<TManSample> handleTManSample = new Handler<TManSample>() {
        @Override
        public void handle(TManSample event) {
            // receive a new list of neighbours
            ArrayList<PeerAddress> sampleNodes = event.getSample();
            // Pick a node or more, and exchange index with them
        }
    };

    Handler<IndexExchangeRequest> handleIndexExchangeRequest = new Handler<IndexExchangeRequest>() {
        @Override
        public void handle(IndexExchangeRequest event) {
            if (event.getMaxIndexID() < maxLuceneIndex) {
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
}
