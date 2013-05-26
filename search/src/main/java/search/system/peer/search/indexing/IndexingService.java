package search.system.peer.search.indexing;

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
import search.simulator.snapshot.Snapshot;
import search.system.peer.search.Search;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Handles the local lucene indexing.
 * Provides methods to add to and query the index. Also supplies index entries for exchanging
 */
public class IndexingService {
    private static final Logger logger = LoggerFactory.getLogger(IndexingService.class);
    // Lucene setup
    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
    Directory index = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);

    // The highest index in the local lucene database
    private int maxLuceneIndex = 0;

    public IndexingService() {
        // An in memory lucene index must be initialized before it can be searched
        try {
            IndexWriter w = new IndexWriter(index, config);
            w.commit();
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxLuceneIndex() {
        return maxLuceneIndex;
    }

    /**
     * Add the documents to the lucene index
     */
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

    /**
     * Add a new entry to the lucene index
     */
    public void addNewEntry(int indexID, String key, String value) throws IOException {
        maxLuceneIndex = indexID;
        Snapshot.updateMaxLeaderIndex(maxLuceneIndex);
        Snapshot.addIndexEntryAtLeader();
        IndexWriter w = new IndexWriter(index, config);
        Document doc = new Document();
        doc.add(new TextField("title", key, Field.Store.YES));
        doc.add(new StringField("id", value, Field.Store.YES));
        doc.add(new StringField("index", String.format("%05d", maxLuceneIndex), Field.Store.YES));
        w.addDocument(doc);
        w.close();
    }

    /**
     * Return a list of all lucene documents with an index higher than sinceIndex
     */
    public List<Document> getDocumentsSinceIndex(int sinceIndex) {
        List<Document> documents = new LinkedList<Document>();

        String queryString = "index:[" + String.format("%05d", sinceIndex) + " TO 99999]";
        Query q = null;
        try {
            q = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(queryString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        IndexSearcher searcher;
        IndexReader reader;
        TopDocs results;
        ScoreDoc[] hits;
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

    /**
     * Query the index for a given string. Return the result as a HTML-table
     */
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
