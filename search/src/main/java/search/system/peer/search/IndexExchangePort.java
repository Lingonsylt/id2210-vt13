package search.system.peer.search;

import cyclon.system.peer.cyclon.CyclonSample;
import se.sics.kompics.PortType;

public final class IndexExchangePort extends PortType {{
    positive(IndexExchangeRequest.class);
    negative(IndexExchangeResponse.class);
}}
