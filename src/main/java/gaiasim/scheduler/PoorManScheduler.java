package gaiasim.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;

import gaiasim.mmcf.MMCFOptimizer;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.network.SubscribedLink;
import gaiasim.scheduler.Scheduler;
import gaiasim.util.Constants;

import org.graphstream.graph.*;

public class PoorManScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, Flow> flows_ = new HashMap<String, Flow>();

    public PoorManScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(Flow f) {
        for (Pathway p : f.paths_) {
            for (int i = 0; i < p.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(p.node_list_.get(i));
                int dst = Integer.parseInt(p.node_list_.get(i+1));
                links_[src][dst].subscribers_.remove(p);
            }
        }
    }

    public void make_paths(Flow f, ArrayList<Link> link_vals) {
        // This portion is similar to Flow::find_pathway_with_link_allocation in Sim
        ArrayList<Pathway> potential_paths = new ArrayList<Pathway>();
        ArrayList<Pathway> completed_paths = new ArrayList<Pathway>();

        // Find all links in the network from the flow's source that have some bandwidth
        // availible and start paths from them.
        ArrayList<Link> links_to_remove = new ArrayList<Link>();
        for (Link l : link_vals) {
            if (l.src_loc_.equals(f.src_loc_)) {
                Pathway p = new Pathway();
                p.node_list_.add(l.src_loc_);
                p.node_list_.add(l.dst_loc_);
                p.bandwidth_ = l.cur_bw_;

                if (l.dst_loc_.equals(f.dst_loc_)) {
                    completed_paths.add(p);
                }
                else {
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
                    if (!p.dst().equals(l.src_loc_)) {
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
                        new_p.node_list_ = (ArrayList<String>)p.node_list_.clone();
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

    public void progress_flow(Flow f) {
        for (Pathway p : f.paths_) {
            f.transmitted_ += p.bandwidth_ * Constants.SIMULATION_TIMESTEP_SEC;
        }
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

    public void schedule_extra_flows(ArrayList<Coflow> unscheduled_coflows, long timestamp) {
        ArrayList<Flow> unscheduled_flows = new ArrayList<Flow>();
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
                int ldst = Integer.parseInt(p.node_list_.get(i+1));
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
        ArrayList<Coflow> unscheduled_coflows = new ArrayList<Coflow>();
        for (Map.Entry<Coflow, Double> e : cct_list) {
            Coflow c = e.getKey();

            if (remaining_bw() <= 0) {
                unscheduled_coflows.add(c);
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
                assert(link_vals != null);

                // This portion is similar to Flow::make() in Sim
                make_paths(f, link_vals);
                
                // Subscribe the flow's paths to the links it uses
                for (Pathway p : f.paths_) {
                    for (int i = 0; i < p.node_list_.size() - 1; i++) {
                        int src = Integer.parseInt(p.node_list_.get(i));
                        int dst = Integer.parseInt(p.node_list_.get(i+1));
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
        if (!unscheduled_coflows.isEmpty() && remaining_bw() > 0.0) {
            schedule_extra_flows(unscheduled_coflows, timestamp);        
        }
        return flows_;
    }
    
    public ArrayList<Map.Entry<Coflow, Double>> sort_coflows(HashMap<String, Coflow> coflows) throws Exception {
        HashMap<Coflow, Double> cct_map = new HashMap<Coflow, Double>();

        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            if (mmcf_out.completion_time_ != -1.0) {
                cct_map.put(c, mmcf_out.completion_time_);
            }
        }

        ArrayList<Map.Entry<Coflow, Double>> cct_list = new ArrayList<Map.Entry<Coflow, Double>>(cct_map.entrySet());
        Collections.sort(cct_list, new Comparator<Map.Entry<Coflow, Double>>() {
            public int compare(Map.Entry<Coflow, Double> o1, Map.Entry<Coflow, Double> o2) {
                if (o1.getValue() == o2.getValue()) return 0;
                return o1.getValue() < o2.getValue() ? -1 : 1;
            }
        });

        return cct_list;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, Flow> flows) {}
}
