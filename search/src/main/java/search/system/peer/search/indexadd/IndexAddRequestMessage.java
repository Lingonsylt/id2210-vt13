package search.system.peer.search.indexadd;

import common.peer.PeerAddress;
import common.peer.PeerMessage;

import java.util.UUID;


public class IndexAddRequestMessage extends PeerMessage {

    private static final long serialVersionUID = 8493601671018888143L;
    private final UUID requestId;
    private final String key;
    private final String value;

    //-------------------------------------------------------------------
    public IndexAddRequestMessage(UUID requestId, String key, String value, PeerAddress source, PeerAddress destination) {
        super(source, destination);
        this.requestId = requestId;
        this.key = key;
        this.value = value;
    }

    public IndexAddRequestMessage(UUID requestId, String key, String value, PeerAddress source) {
        super(source, source);
        this.requestId = requestId;
        this.key = key;
        this.value = value;
    }

    //-------------------------------------------------------------------
    public UUID getRequestId() {
        return requestId;
    }

    //-------------------------------------------------------------------
    public String getKey() {
        return key;
    }

    public String getValue() {
        return key;
    }

    //-------------------------------------------------------------------
    public int getSize() {
        return 0;
    }
}
