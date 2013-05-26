package search.system.peer.search.leaderelection;

import common.peer.PeerAddress;
import common.peer.PeerMessage;

import java.util.UUID;


public class LeaderElectionMessage extends PeerMessage {

    private static final long serialVersionUID = 8493601671018888143L;
    private final UUID requestId;
    // Possible values for command:
    // AM_I_LEGEND: Request for a leader election vote
    // I_AM_LEGEND: The sender is announcing it's the new leader
    // YOU_ARE_LEGEND: Yes vote in leader election
    // YOU_ARE_LOSER: No vote in leader election
    // ARE_YOU_ALIVE: Heartbeat request
    // I_AM_ALIVE: Heartbeat response
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

    public int getNextId() {
        return nextId;
    }

    //-------------------------------------------------------------------
    public UUID getRequestId() {
        return requestId;
    }

    //-------------------------------------------------------------------
    public String getCommand() {
        return command;
    }
}
