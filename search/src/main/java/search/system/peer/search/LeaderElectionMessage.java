package search.system.peer.search;

import common.peer.PeerAddress;
import common.peer.PeerMessage;

import java.util.UUID;


public class LeaderElectionMessage extends PeerMessage {

    private static final long serialVersionUID = 8493601671018888143L;
    private final UUID requestId;
    private final String command;
    private final int nextId;

    //-------------------------------------------------------------------
    public LeaderElectionMessage(UUID requestId, String command, PeerAddress source, PeerAddress destination) {
        this(requestId, command, 0, source, destination);
    }

    public LeaderElectionMessage(UUID requestId, String command, int nextId, PeerAddress source, PeerAddress destination) {
        super(source, destination);
        this.requestId = requestId;
        this.nextId = nextId;
        this.command = command;
    }



    //-------------------------------------------------------------------
    public UUID getRequestId() {
        return requestId;
    }

    //-------------------------------------------------------------------
    public String getCommand() {
        return command;
    }

    //-------------------------------------------------------------------
    public int getSize() {
        return 0;
    }
}
