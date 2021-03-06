package common.simulation.scenarios;

import common.simulation.scenarios.Operations;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

import java.math.BigInteger;
import java.util.Date;

@SuppressWarnings("serial")
public class Scenario1 extends Scenario {
    private static int numberOfPeers = System.getenv("PEERS") != null ? Integer.parseInt(System.getenv("PEERS")) : 200;
	private static SimulationScenario scenario = new SimulationScenario() {{

		StochasticProcess process1 = new StochasticProcess() {{
			eventInterArrivalTime(constant(50));
			raise(numberOfPeers, Operations.peerJoin(5), uniform(13));
		}};

		StochasticProcess process2 = new StochasticProcess() {{
			eventInterArrivalTime(constant(100));
			raise(1, Operations.peerFail(new BigInteger("1")), uniform(13));
		}};

        StochasticProcess process3 = new StochasticProcess() {{
            eventInterArrivalTime(constant(100));
            raise(1, Operations.addIndexEntry("key", "value"), uniform(13));
        }};
/*
        StochasticProcess process4 = new StochasticProcess() {{
            eventInterArrivalTime(constant(100));
            raise(50, Operations.peerJoin(5), uniform(13));
        }};
               */
		process1.start();

		process2.startAfterTerminationOf(100000, process1);
        process3.startAfterTerminationOf(100000, process2);

		//process3.startAfterTerminationOf(2000, process2);
		//process4.startAfterTerminationOf(2000, process3);
		//process2.startAfterTerminationOf(2000, process1);
	}};
	
//-------------------------------------------------------------------
	public Scenario1() {
		super(scenario);
	} 
}
