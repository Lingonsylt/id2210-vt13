package search.system.peer.search.indexadd;

import common.peer.PeerAddress;
import common.peer.PeerMessage;

import java.util.UUID;


public class LeaderResponseMessage extends PeerMessage {

    private static final long serialVersionUID = 8493601671018888143L;
    private final UUID requestId;

    //-------------------------------------------------------------------
    public LeaderResponseMessage(UUID requestId, PeerAddress source, PeerAddress destination) {
        super(source, destination);
        this.requestId = requestId;
    }

    //-------------------------------------------------------------------
    public UUID getRequestId() {
        return requestId;
    }

    //-------------------------------------------------------------------
    public int getSize() {
        return 0;
    }
}
