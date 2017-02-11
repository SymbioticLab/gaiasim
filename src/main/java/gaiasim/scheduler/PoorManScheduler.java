package gaiasim.scheduler;

import java.util.List;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.Scheduler;

import org.graphstream.graph.*;

public class PoorManScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, Flow> flows_ = new HashMap<String, Flow>();

    public PoorManScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(Flow f) {}

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows, 
                                                long timestamp) {
        flows_.clear();
        return flows_;
    }

    public double remaining_bw() {
        double remaining_bw = 0.0;
        for (int i = 0; i < net_graph_.nodes_.size() + 1; i++) {
            for (int j = 0; j < net_graph_.nodes_.size() + 1; j++) {
                if (links_[i][j] != null) {
                    remaining_bw += links_[i][j].remaining_bw();
                }
            }
        }

        return remaining_bw;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, Flow> flows) {}
}
