package search.simulator.core;

import common.peer.PeerAddress;
import common.peer.PeerMessage;

import java.util.UUID;


public class SimulationAddIndexEntry extends PeerMessage {

    private static final long serialVersionUID = 8493601671018888143L;
    private final String key;
    private final String value;

    //-------------------------------------------------------------------
    public SimulationAddIndexEntry(String key, String value, PeerAddress source, PeerAddress destination) {
        super(source, destination);
        this.key = key;
        this.value = value;
    }

    public SimulationAddIndexEntry(String key, String value, PeerAddress source) {
        super(source, source);
        this.key = key;
        this.value = value;
    }


    //-------------------------------------------------------------------
    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    //-------------------------------------------------------------------
    public int getSize() {
        return 0;
    }
}
