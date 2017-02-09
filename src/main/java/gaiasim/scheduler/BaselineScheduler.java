package gaiasim.scheduler;

import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.Scheduler;

import org.graphstream.graph.*;

public class BaselineScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, Flow> flows_ = new HashMap<String, Flow>();

    // All possible links in our graph
    public Link[][] links_;

    public BaselineScheduler(NetGraph net_graph) {
        super(net_graph);

        links_ = new Link[net_graph_.nodes_.size() + 1][net_graph_.nodes_.size() + 1];
        for (Edge e : net_graph_.graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());
            links_[src][dst] = new Link(Double.parseDouble(e.getAttribute("bandwidth").toString()));
        }
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows, 
                                                long timestamp) {
        flows_.clear();
        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);

            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);

                // TODO(jack): Actually ad scheduling part to get rates for flows
                f.rate_ = 10.0;

                if (f.start_timestamp_ == -1) {
                    f.start_timestamp_ = timestamp;
                }

                flows_.put(f.id_, f);
            }
        }

        return flows_;
    }
}
