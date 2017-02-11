package gaiasim.scheduler;

import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;

import org.graphstream.graph.Edge;

public abstract class Scheduler {
    public NetGraph net_graph_;

    // All possible links in our graph
    public Link[][] links_;

    public Scheduler(NetGraph net_graph) {
        net_graph_ = net_graph;

        links_ = new Link[net_graph_.nodes_.size() + 1][net_graph_.nodes_.size() + 1];
        for (Edge e : net_graph_.graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());
            links_[src][dst] = new Link(Double.parseDouble(e.getAttribute("bandwidth").toString()));
            links_[dst][src] = new Link(Double.parseDouble(e.getAttribute("bandwidth").toString()));
        }
    }

    public abstract void finish_flow(Flow f);
 
    public abstract HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                         long timestamp);

    public abstract void update_flows(HashMap<String, Flow> flows);
}
