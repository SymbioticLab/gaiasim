package gaiasim.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.util.Constants;

public class BaselineScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hold new data rather than creating another
    // new map object (avoid GC).
    protected HashMap<String, Flow> flows_ = new HashMap<String, Flow>();

    public BaselineScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(Flow f) {
        ArrayList<String> nodes = f.paths_.get(0).node_list_;
        for (int i = 0; i < nodes.size()- 1; i++) {
            int src = Integer.parseInt(nodes.get(i));
            int dst = Integer.parseInt(nodes.get(i+1));
            links_[src][dst].subscribers_.remove(f.paths_.get(0));
        }
    }

    public void progress_flow(Flow f) {
        f.transmitted_ += f.rate_ * Constants.SIMULATION_TIMESTEP_SEC;
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows, 
                                                long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);

            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);
                if (f.done_) {
                    continue;
                }
                
                Pathway p = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
                //Pathway p = net_graph_.apmb_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)];
                f.paths_.clear();
                f.paths_.add(p);
              
                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i+1));
                    links_[src][dst].subscribers_.addAll(f.paths_);
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

            ArrayList<String> nodes = f.paths_.get(0).node_list_;
            for (int i = 0; i < nodes.size() - 1; i++) {
                int src = Integer.parseInt(nodes.get(i));
                int dst = Integer.parseInt(nodes.get(i+1));
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }

            f.rate_ = min_bw;
            f.paths_.get(0).bandwidth_ = min_bw;
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and remaining volume " + (f.volume_ - f.transmitted_) + " on path " + f.paths_.get(0));
        }

        return flows_;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, Flow> flows) {
        for (String k : flows.keySet()) {
            Flow f = flows.get(k);

            double min_bw = Double.MAX_VALUE;

            ArrayList<String> nodes = f.paths_.get(0).node_list_;
            for (int i = 0; i < nodes.size() - 1; i++) {
                int src = Integer.parseInt(nodes.get(i));
                int dst = Integer.parseInt(nodes.get(i+1));
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }

            f.rate_ = min_bw;
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and remaining volume " + (f.volume_ - f.transmitted_) + " on path " + f.paths_.get(0));
        }
    }
}
