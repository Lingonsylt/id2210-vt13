package common.simulation;

import se.sics.kompics.Event;

import java.math.BigInteger;

public final class AddIndexEntry extends Event {

	private final String key;
	private final String value;

//-------------------------------------------------------------------
	public AddIndexEntry(String key, String value) {
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
}
