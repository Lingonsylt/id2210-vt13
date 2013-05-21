package search.simulator.snapshot;

import java.util.ArrayList;

import common.peer.PeerAddress;
import search.system.peer.search.Search;

public class PeerInfo {
	private double num;
	private ArrayList<PeerAddress> cyclonPartners;
    private Search search;

//-------------------------------------------------------------------
	public PeerInfo() {
		this.cyclonPartners = new ArrayList<PeerAddress>();
	}

//-------------------------------------------------------------------
	public void updateNum(double num) {
		this.num = num;
	}

    public void updateSearch(Search search) {
        this.search = search;
    }

//-------------------------------------------------------------------
	public void updateNum(int num) {
		this.num = num;
	}

//-------------------------------------------------------------------
	public void updateCyclonPartners(ArrayList<PeerAddress> partners) {
		this.cyclonPartners = partners;
	}

//-------------------------------------------------------------------
	public double getNum() {
		return this.num;
	}

    public Search getSearch() {
        return search;
    }

//-------------------------------------------------------------------
	public ArrayList<PeerAddress> getCyclonPartners() {
		return this.cyclonPartners;
	}
}
