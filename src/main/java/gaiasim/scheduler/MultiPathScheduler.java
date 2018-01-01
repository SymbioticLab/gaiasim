package gaiasim.scheduler;

import gaiasim.mmcf.LoadBalanceOptimizer;
import gaiasim.network.*;

import java.util.ArrayList;
import java.util.HashMap;

public class MultiPathScheduler extends PoorManScheduler {
    public MultiPathScheduler(NetGraph net_graph) {
        super(net_graph);
    }

    // Key difference from PoorManScheduler's schedule_flows is combining all coflows to one
    @Override
    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        // Collapse all coflows to one
        Coflow combined_coflow = new Coflow("COMBINED", null);
        combined_coflow.volume_ = 0.0;
        int combined_flow_int_id = 0;
        HashMap<Integer, Integer> combined_to_original_int_id = new HashMap<>();
        for (Coflow coflow : coflows.values()) {
            for (Flow flow : coflow.flows_.values()) {
                if (!flow.done_) {
                    // Serializing flow_int_id_ values for the optimizer.
                    // Put back after the optimizer results have been parsed.
                    combined_to_original_int_id.put(combined_flow_int_id, flow.int_id_);
                    flow.int_id_ = combined_flow_int_id;
                    combined_flow_int_id++;

                    combined_coflow.volume_ += flow.volume_;
                    combined_coflow.flows_.put(flow.id_, flow);
                }
            }
        }

        // Find paths for each flow
        LoadBalanceOptimizer.LoadBalanceOutput mf_out = LoadBalanceOptimizer.glpk_optimize(combined_coflow, net_graph_, links_);

        for (Flow f : combined_coflow.flows_.values()) {
            ArrayList<Link> link_vals = mf_out.flow_link_bw_map_.get(f.int_id_);

            // Fix int_id_ of the flow
            f.int_id_ = combined_to_original_int_id.get(f.int_id_);

            if (link_vals != null) {
                make_paths(f, link_vals);
            }

            if (f.paths_.size() == 0) {
                // Select the shortest path if nothing else is found
                f.paths_.add(new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]));
            }

            // Subscribe the flow's paths to the links it uses
            for (Pathway p : f.paths_) {
                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i + 1));
                    links_[src][dst].subscribers_.add(p);
                }
            }

            System.out.println("Adding flow " + f.id_ + " remaining = " + f.remaining_volume());
            System.out.println("  has pathways: ");
            for (Pathway p : f.paths_) {
                System.out.println("    " + p.toString());
            }

            if (f.start_timestamp_ == -1) {
                System.out.println("Setting start_timestamp to " + timestamp);
                f.start_timestamp_ = timestamp;
            }

            flows_.put(f.id_, f);
        }

        // Must call this because we cannot use rates from the optimization, only paths
        update_flows(flows_);

        return flows_;
    }

    @Override
    public void update_flows(HashMap<String, Flow> flows) {
        for (Flow f : flows.values()) {
            f.rate_ = 0;
            for (Pathway p : f.paths_) {
                double min_bw = Double.MAX_VALUE;
                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i + 1));
                    double link_bw = links_[src][dst].bw_per_flow();

                    if (link_bw < min_bw) {
                        min_bw = link_bw;
                    }
                }

                if (min_bw < 0) {
                    min_bw = 0;
                    System.err.println("WARNING: min_bw < 0.");
                }

                p.bandwidth_ = min_bw;
                f.rate_ += min_bw;
            }
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and remaining volume " + (f.volume_ - f.transmitted_) + " on path " + f.paths_toString());
        }
    }
}
