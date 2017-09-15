package gaiasim.scheduler;

import gaiasim.mmcf.LoadBalanceOptimizer;
import gaiasim.network.*;

import java.util.ArrayList;
import java.util.HashMap;

public class SwanScheduler extends PoorManScheduler {
    private static long REMAP_INTERVAL_MS = 5 * 60 * 1000;
    // Remember when was the last time we remapped paths for the flows;
    // If it's more than REMAP_INTERVAL_MS milliseconds, lets remap.
    // By default, SWAN remaps every 5 minutes.
    private long last_remap_timestamp;

    public SwanScheduler(NetGraph net_graph) {
        super(net_graph);
        last_remap_timestamp = 0;
    }

    // Key difference from MultiPathScheduler's schedule_flows is using one path for each flow
    @Override
    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                long timestamp) throws Exception {
        boolean remap_flows = false;
        if (timestamp - last_remap_timestamp >= REMAP_INTERVAL_MS) {
            remap_flows = true;
            last_remap_timestamp = timestamp;
        }

        flows_.clear();
        reset_links();

        // Keep track of flows that are already mapped
        ArrayList<Flow> old_flows = new ArrayList<>();

        // Collapse all coflows to one
        Coflow combined_coflow = new Coflow("COMBINED", null);
        combined_coflow.volume_ = 0.0;
        int combined_flow_int_id = 0;
        HashMap<Integer, Integer> combined_to_original_int_id = new HashMap<>();
        for (Coflow coflow : coflows.values()) {
            for (Flow flow : coflow.flows_.values()) {
                if (!flow.done_) {
                    if (remap_flows || flow.max_bw_path == null) {
                        // Serializing flow_int_id_ values for the optimizer.
                        // Put back after the optimizer results have been parsed.
                        combined_to_original_int_id.put(combined_flow_int_id, flow.int_id_);
                        flow.int_id_ = combined_flow_int_id;
                        combined_flow_int_id++;

                        combined_coflow.volume_ += flow.volume_;
                        combined_coflow.flows_.put(flow.id_, flow);
                    } else {
                        old_flows.add(flow);
                    }
                }
            }
        }

        // Find paths for each flow
        LoadBalanceOptimizer.MaxFlowOutput mf_out = LoadBalanceOptimizer.glpk_optimize(combined_coflow, net_graph_, links_);

        for (Flow f : combined_coflow.flows_.values()) {
            ArrayList<Link> link_vals = mf_out.flow_link_bw_map_.get(f.int_id_);

            // Fix int_id_ of the flow
            f.int_id_ = combined_to_original_int_id.get(f.int_id_);

            // Remap if we get a new mapping
            if (link_vals != null) {
                make_paths(f, link_vals);

                // Select one path for the flow
                Pathway max_bw_path = null;
                double max_bw = 0.0;
                for (Pathway p : f.paths_) {
                    if (p.bandwidth_ > max_bw) {
                        max_bw = p.bandwidth_;
                        max_bw_path = p;
                    }
                }

                // Remember the selected path for the future until it's remapped
                f.max_bw_path = max_bw_path;
            }

            if (f.max_bw_path == null || f.rate_ == 0.0) {
                // Select the shortest path if nothing else is found
                f.max_bw_path = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
            }

            // Subscribe the flow's paths to the links it uses on the selected path
            for (int i = 0; i < f.max_bw_path.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(f.max_bw_path.node_list_.get(i));
                int dst = Integer.parseInt(f.max_bw_path.node_list_.get(i + 1));
                links_[src][dst].subscribers_.add(f.max_bw_path);
            }

            System.out.println("Adding flow " + f.id_ + " remaining = " + f.remaining_volume());
            System.out.println("  has pathways: ");
            System.out.println("    " + f.max_bw_path.toString());

            if (f.start_timestamp_ == -1) {
                System.out.println("Setting start_timestamp to " + timestamp);
                f.start_timestamp_ = timestamp;
            }

            flows_.put(f.id_, f);
        }

        // Add back flows that were already mapped
        for (Flow of : old_flows) {
            flows_.put(of.id_, of);
            for (int i = 0; i < of.max_bw_path.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(of.max_bw_path.node_list_.get(i));
                int dst = Integer.parseInt(of.max_bw_path.node_list_.get(i + 1));
                links_[src][dst].subscribers_.add(of.max_bw_path);
            }
        }

        // Must call this because we cannot use rates from the optimization, only paths
        update_flows(flows_);

        return flows_;
    }

    @Override
    public void update_flows(HashMap<String, Flow> flows) {
        for (Flow f : flows.values()) {
            f.rate_ = 0;
            f.paths_.clear();
            f.paths_.add(f.max_bw_path);
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
                p.bandwidth_ = min_bw;
                f.rate_ += min_bw;
            }
            System.out.println("Flow " + f.id_ + " has rate " + f.rate_ + " and remaining volume " + (f.volume_ - f.transmitted_) + " on path " + f.paths_.get(0));
        }
    }
}
