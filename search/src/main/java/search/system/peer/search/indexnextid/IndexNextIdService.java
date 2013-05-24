package search.system.peer.search.indexnextid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexNextIdService {
    private static final Logger logger = LoggerFactory.getLogger(IndexNextIdService.class);

    // If leader, the next id that should be used when adding an entry to lucene
    private int nextId = 0;

    public IndexNextIdService() {
    }

    public int getIncrementedId() {
        return ++nextId;
    }

    public int getNextId() {
        return nextId;
    }

    public void setNextId(int nextId) {
        this.nextId = nextId;
    }
}
