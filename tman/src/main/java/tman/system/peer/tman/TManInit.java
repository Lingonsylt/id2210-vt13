package tman.system.peer.tman;

import common.configuration.CyclonConfiguration;
import common.configuration.TManConfiguration;
import common.peer.PeerAddress;
import se.sics.kompics.Init;

public final class TManInit extends Init {

	private final PeerAddress peerSelf;
	private final TManConfiguration configuration;
    private final CyclonConfiguration cycloneConfiguration;

//-------------------------------------------------------------------
	public TManInit(PeerAddress peerSelf, TManConfiguration configuration, CyclonConfiguration cycloneConfiguration) {
		super();
		this.peerSelf = peerSelf;

		this.configuration = configuration;
        this.cycloneConfiguration = cycloneConfiguration;
    }

//-------------------------------------------------------------------
	public PeerAddress getSelf() {
		return this.peerSelf;
	}

//-------------------------------------------------------------------
	public TManConfiguration getConfiguration() {
		return this.configuration;
	}

    public CyclonConfiguration getCyclonConfiguration() {
        return this.cycloneConfiguration;
    }
}