package search.system.peer.search;

import common.peer.PeerAddress;
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
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Positive;
import se.sics.kompics.timer.Timer;
import search.simulator.snapshot.Snapshot;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

public class IndexingService {
    private static final Logger logger = LoggerFactory.getLogger(IndexingService.class);

    // Dependencies
    private PeerAddress self;
    private Positive<Timer> timerPort;
    Search.TriggerDependency sc;

    // Lucene setup
    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    Directory index = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // The highest index in the local lucene database
    private int maxLuceneIndex = 0;

    public IndexingService(PeerAddress self, Positive<Timer> timerPort) {
        this.sc = sc;
        this.self = self;
        this.timerPort = timerPort;

        // An in memory lucene index must be initialized before it can be searched
        try {
            IndexWriter w = new IndexWriter(index, config);
            w.commit();
            w.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void setSelf(PeerAddress self) {
        this.self = self;
    }

    public int getMaxLuceneIndex() {
        return maxLuceneIndex;
    }

    public void addDocuments(List<Document> documents) throws IOException {
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

    public void addNewEntry(int indexID, String key, String value) throws IOException {
        maxLuceneIndex = indexID;
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

    public List<Document> getDocumentsSinceIndex(int sinceIndex) {
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

    public String query(String queryString) throws ParseException, IOException {
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
}
