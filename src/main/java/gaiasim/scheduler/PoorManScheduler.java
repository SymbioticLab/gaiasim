package gaiasim.scheduler;

import gaiasim.mmcf.MMCFOptimizer;
import gaiasim.mmcf.MaxFlowOptimizer;
import gaiasim.network.*;
import gaiasim.util.Constants;

import java.util.*;

public class PoorManScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    protected HashMap<String, Flow> flows_ = new HashMap<>();

    public PoorManScheduler(NetGraph net_graph) {
        super(net_graph);
    }

    public void finish_flow(Flow f) {
        for (Pathway p : f.paths_) {
            for (int i = 0; i < p.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(p.node_list_.get(i));
                int dst = Integer.parseInt(p.node_list_.get(i + 1));
                links_[src][dst].subscribers_.remove(p);
            }
        }
    }

    public void make_paths(Flow f, ArrayList<Link> link_vals) {
        // This portion is similar to Flow::find_pathway_with_link_allocation in Sim
        ArrayList<Pathway> potential_paths = new ArrayList<Pathway>();
        ArrayList<Pathway> completed_paths = new ArrayList<Pathway>();

        // Find all links in the network from the flow's source that have some bandwidth
        // available and start paths from them.
        ArrayList<Link> links_to_remove = new ArrayList<Link>();
        for (Link l : link_vals) {
            if (l.src_loc_.equals(f.src_loc_)) {
                Pathway p = new Pathway();
                p.node_list_.add(l.src_loc_);
                p.node_list_.add(l.dst_loc_);
                p.bandwidth_ = l.cur_bw_;

                if (l.dst_loc_.equals(f.dst_loc_)) {
                    completed_paths.add(p);
                } else {
                    potential_paths.add(p);
                }

                links_to_remove.add(l);
            }
        }

        // Remove any Links that were added above
        for (Link l : links_to_remove) {
            link_vals.remove(l);
        }

        // Iterate through remaining links and try to add them to paths
        ArrayList<Pathway> paths_to_remove = new ArrayList<Pathway>();
        boolean link_added;
        while (!link_vals.isEmpty()) {
            links_to_remove.clear();
            link_added = false;

            for (Link l : link_vals) {
                if (l.cur_bw_ == 0.0) {
                    links_to_remove.add(l);
                    continue;
                }

                for (Pathway p : potential_paths) {
                    // Does this link fit after the current last node in the path?
                    if (!p.last_node().equals(l.src_loc_)) {
                        continue;
                    }

                    // Does the bandwidth available on this link directly match the bandwidth
                    // of this pathway?
                    if (Math.round(Math.abs(p.bandwidth_ - l.cur_bw_) * 100.0) / 100.0 < 0.01) {
                        p.node_list_.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.dst_loc_)) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does this link have less bandwidth than the bandwidth available on the path?
                    // Split the path in two -- one path taking this link (and reducing its bandwidth)
                    // and the other not taking the path and using the remaining bandwidth.
                    else if (Math.round((p.bandwidth_ - l.cur_bw_) * 100.0) / 100.0 >= 0.01) {
                        Pathway new_p = new Pathway();
                        new_p.bandwidth_ = p.bandwidth_ - l.cur_bw_;
                        new_p.node_list_ = (ArrayList<String>) p.node_list_.clone();
                        potential_paths.add(new_p);
                        p.bandwidth_ = l.cur_bw_;
                        p.node_list_.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.dst_loc_)) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does the link have more bandwidth than the bandwidth available on the path?
                    // Only reduce the link's bandwidth by the amount that could be used by the path.
                    else if (Math.round((p.bandwidth_ - l.cur_bw_) * 100.0) / 100.0 <= -0.01) {
                        l.cur_bw_ = l.cur_bw_ - p.bandwidth_;
                        p.node_list_.add(l.dst_loc_);
                        link_added = true;
                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.dst_loc_)) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }
                        // TODO(jack): Consider breaking here -- old simulator does so...
                    }

                } // for pathway

                // Remove any paths that have been completed during this last round
                for (Pathway p : paths_to_remove) {
                    potential_paths.remove(p);
                }

            } // for link

            // Remove any Links that were added above
            for (Link l : links_to_remove) {
                link_vals.remove(l);
            }

            // If we were unable to add any links this round, just quit
            if (!link_added) {
                break;
            }
        } // while link_vals
        f.paths_.clear();
        f.paths_ = completed_paths;
    }

    public double progress_flow(Flow f) {
        double totalBW = 0.0;
        for (Pathway p : f.paths_) {
            f.transmitted_ += p.bandwidth_ * Constants.SIMULATION_TIMESTEP_SEC;
            totalBW += p.bandwidth_;
        }
        return totalBW;
    }

    private double remaining_bw() {
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

    private void schedule_extra_flows(ArrayList<Coflow> unscheduled_coflows, long timestamp) throws Exception {
        // Collapse all coflows to one
        Coflow combined_coflow = new Coflow("COMBINED", null);
        combined_coflow.volume_ = 0.0;
        int combined_flow_int_id = 0;
        HashMap<Integer, Integer> combined_to_original_int_id = new HashMap<>();
        for (Coflow coflow : unscheduled_coflows) {
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
        MaxFlowOptimizer.MaxFlowOutput mf_out = MaxFlowOptimizer.glpk_optimize(combined_coflow, net_graph_, links_);

        int[][] subscriber_counts = new int[net_graph_.nodes_.size() + 1][net_graph_.nodes_.size() + 1];

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

                // Remember the selected path
                f.max_bw_path = max_bw_path;
            }

            // Select the shortest path if nothing else is found
            if (f.max_bw_path == null || f.rate_ == 0.0) {
                f.max_bw_path = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
            }

            // Subscribe the flow's paths to the links it uses on the selected path
            for (int i = 0; i < f.max_bw_path.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(f.max_bw_path.node_list_.get(i));
                int dst = Integer.parseInt(f.max_bw_path.node_list_.get(i + 1));
                links_[src][dst].subscribers_.add(f.max_bw_path);
                subscriber_counts[src][dst]++;
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

        // Must fix bandwidth allocation because we cannot use rates from the optimization, only paths
        for (Flow f : combined_coflow.flows_.values()) {
            f.rate_ = 0;
            f.paths_.clear();
            f.paths_.add(f.max_bw_path);
            for (Pathway p : f.paths_) {
                double min_bw = Double.MAX_VALUE;
                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i + 1));
                    double link_bw = links_[src][dst].remaining_bw() / subscriber_counts[src][dst];

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

    private void schedule_extra_flows_old(ArrayList<Coflow> unscheduled_coflows, long timestamp) {
        ArrayList<Flow> unscheduled_flows = new ArrayList<>();
        for (Coflow c : unscheduled_coflows) {
            for (String k : c.flows_.keySet()) {
                Flow f = c.flows_.get(k);
                if (f.remaining_volume() > 0) {
                    unscheduled_flows.add(c.flows_.get(k));
                }
            }
        }
        Collections.sort(unscheduled_flows, new Comparator<Flow>() {
            public int compare(Flow o1, Flow o2) {
                if (o1.volume_ == o2.volume_) return 0;
                return o1.volume_ < o2.volume_ ? -1 : 1;
            }
        });

        for (Flow f : unscheduled_flows) {
            int src = Integer.parseInt(f.src_loc_);
            int dst = Integer.parseInt(f.dst_loc_);
            Pathway p = new Pathway(net_graph_.apsp_[src][dst]);

            double min_bw = Double.MAX_VALUE;
            SubscribedLink[] path_links = new SubscribedLink[p.node_list_.size() - 1];
            for (int i = 0; i < p.node_list_.size() - 1; i++) {
                int lsrc = Integer.parseInt(p.node_list_.get(i));
                int ldst = Integer.parseInt(p.node_list_.get(i + 1));
                SubscribedLink l = links_[lsrc][ldst];

                double bw = l.remaining_bw();
                path_links[i] = l;
                if (bw < min_bw) {
                    min_bw = bw;
                }
            }

            if (min_bw > 0) {
                p.bandwidth_ = min_bw;

                for (SubscribedLink l : path_links) {
                    l.subscribers_.add(p);
                }
                f.paths_.clear();
                f.paths_.add(p);

                System.out.println("Adding separate flow " + f.id_ + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway path : f.paths_) {
                    System.out.println("    " + path.toString());
                }

                if (f.start_timestamp_ == -1) {
                    f.start_timestamp_ = timestamp;
                }
                flows_.put(f.id_, f);
            }
        }
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                long timestamp) throws Exception {
        flows_.clear();
        reset_links();
        ArrayList<Map.Entry<Coflow, Double>> cct_list = sort_coflows(coflows);
        ArrayList<Coflow> unscheduled_coflows = new ArrayList<>();
        boolean no_bw_remains = false;
        for (Map.Entry<Coflow, Double> e : cct_list) {
            Coflow c = e.getKey();

            if (no_bw_remains || remaining_bw() <= 0) {
                unscheduled_coflows.add(c);
                no_bw_remains = true;
                continue;
            }

            System.out.println("Coflow " + c.id_ + " expected to complete in " + e.getValue());

            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);

            boolean all_flows_scheduled = true;
            for (String k : c.flows_.keySet()) {
                int id = c.flows_.get(k).int_id_;
                all_flows_scheduled = all_flows_scheduled && (mmcf_out.flow_link_bw_map_.get(id) != null);
            }

            if (mmcf_out.completion_time_ == -1.0 || !all_flows_scheduled) {
                unscheduled_coflows.add(c);
                continue;
            }

            // This portion is similar to CoFlow::make() in Sim
            for (String k : c.flows_.keySet()) {
                Flow f = c.flows_.get(k);
                if (f.done_) {
                    continue;
                }

                ArrayList<Link> link_vals = mmcf_out.flow_link_bw_map_.get(f.int_id_);
                assert (link_vals != null);

                // This portion is similar to Flow::make() in Sim
                make_paths(f, link_vals);

                // Select the shortest path if nothing else is found
                if (f.paths_.size() == 0) {
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
        }

        // Schedule any available flows
        if (!unscheduled_coflows.isEmpty() && !no_bw_remains) {
            schedule_extra_flows(unscheduled_coflows, timestamp);
        }
        return flows_;
    }

    private ArrayList<Map.Entry<Coflow, Double>> sort_coflows(HashMap<String, Coflow> coflows) throws Exception {
        HashMap<Coflow, Double> cct_map = new HashMap<>();

        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            if (mmcf_out.completion_time_ != -1.0) {
                cct_map.put(c, mmcf_out.completion_time_);
            }
        }

        ArrayList<Map.Entry<Coflow, Double>> cct_list = new ArrayList<>(cct_map.entrySet());
        Collections.sort(cct_list, new Comparator<Map.Entry<Coflow, Double>>() {
            public int compare(Map.Entry<Coflow, Double> o1, Map.Entry<Coflow, Double> o2) {
                if (o1.getValue() == o2.getValue()) return 0;
                return o1.getValue() < o2.getValue() ? -1 : 1;
            }
        });

        return cct_list;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, Flow> flows) {
    }
}
