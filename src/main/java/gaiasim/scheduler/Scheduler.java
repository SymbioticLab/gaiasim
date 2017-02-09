package gaiasim.scheduler;

import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;

public abstract class Scheduler {
    public NetGraph net_graph_;

    public Scheduler(NetGraph net_graph) {
        net_graph_ = net_graph;
    }

    public abstract HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                         long timestamp);
}
