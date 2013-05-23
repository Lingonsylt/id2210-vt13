package common.simulation.scenarios;

import common.simulation.PeerFail;
import common.simulation.PeerJoin;
import common.simulation.Publish;
import common.simulation.AddIndexEntry;
import java.math.BigInteger;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;

@SuppressWarnings("serial")
public class Operations {
    private static int numberOfPeers = System.getenv("PEERS") != null ? Integer.parseInt(System.getenv("PEERS")) : 200;
    private static BigInteger peerId = new BigInteger(numberOfPeers + "").add(BigInteger.ONE);
//-------------------------------------------------------------------
	public static Operation1<PeerJoin, BigInteger> peerJoin(final int num) {
		return new Operation1<PeerJoin, BigInteger>() {
			public PeerJoin generate(BigInteger id) {
                peerId = peerId.subtract(BigInteger.ONE);
				return new PeerJoin(peerId, num);
			}
		};
	}

//-------------------------------------------------------------------
	public static Operation1<PeerFail, BigInteger> peerFail(final BigInteger staticId) {
        return new Operation1<PeerFail, BigInteger>() {
            public PeerFail generate(BigInteger id) {
                return new PeerFail(staticId);
            }
        };
    }

    public static Operation1<AddIndexEntry, BigInteger> addIndexEntry(final String key, final String value) {
        return new Operation1<AddIndexEntry, BigInteger>() {
            public AddIndexEntry generate(BigInteger id) {
                return new AddIndexEntry(key, value);
            }
        };
    }

//-------------------------------------------------------------------
	public static Operation<Publish> publish = new Operation<Publish>() {
		public Publish generate() {
			return new Publish();
		}
	};
}
