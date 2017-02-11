package gaiasim.scheduler;

import java.util.List;
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

    public BaselineScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(Flow f) {
        List<Node> path_nodes = f.path_.getNodePath();
        for (int i = 0; i < f.path_.getNodeCount() - 1; i++) {
            int src = Integer.parseInt(path_nodes.get(i).toString());
            int dst = Integer.parseInt(path_nodes.get(i+1).toString());
            links_[src][dst].subscribers_.remove(f);
        }
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows, 
                                                long timestamp) {
        flows_.clear();
        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);

            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);

                f.path_ = net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)];
               
                List<Node> path_nodes = f.path_.getNodePath();
                for (int i = 0; i < f.path_.getNodeCount() - 1; i++) {
                    int src = Integer.parseInt(path_nodes.get(i).toString());
                    int dst = Integer.parseInt(path_nodes.get(i+1).toString());
                    links_[src][dst].subscribers_.add(f);
                }

                if (f.start_timestamp_ == -1) {
                    f.start_timestamp_ = timestamp;
                }

                flows_.put(f.id_, f);
            }
        }
        
        for (String k : flows_.keySet()) {
            Flow f = flows_.get(k);

            double min_bw = Double.MAX_VALUE;

            List<Node> path_nodes = f.path_.getNodePath();
            for (int i = 0; i < f.path_.getNodeCount() - 1; i++) {
                int src = Integer.parseInt(path_nodes.get(i).toString());
                int dst = Integer.parseInt(path_nodes.get(i+1).toString());
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }

            f.rate_ = min_bw;
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and volume " + f.volume_ + " on path " + f.path_);
        }

        return flows_;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, Flow> flows) {
        for (String k : flows.keySet()) {
            Flow f = flows.get(k);

            double min_bw = Double.MAX_VALUE;

            List<Node> path_nodes = f.path_.getNodePath();
            for (int i = 0; i < f.path_.getNodeCount() - 1; i++) {
                int src = Integer.parseInt(path_nodes.get(i).toString());
                int dst = Integer.parseInt(path_nodes.get(i+1).toString());
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }

            f.rate_ = min_bw;
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and remaining volume " + (f.volume_ - f.transmitted_) + " on path " + f.path_);
        }
    }
}
