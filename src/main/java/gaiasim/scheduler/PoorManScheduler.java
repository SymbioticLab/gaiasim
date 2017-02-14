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
                                                long timestamp) throws Exception {
        flows_.clear();
        ArrayList<Map.Entry<Coflow, Double>> cct_list = sort_coflows(coflows);
        for (Map.Entry<Coflow, Double> e : cct_list) {
            if (remaining_bw() <= 0) {
                break;
            }

            Coflow c = e.getKey();
            System.out.println("Coflow " + c.id_ + " expected to complete at " + e.getValue());

            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            
            // This portion is similar to CoFlow::make() in Sim
            for (String k : c.flows_.keySet()) {
                Flow f = c.flows_.get(k);

                ArrayList<Link> link_vals = mmcf_out.flow_link_bw_map_.get(f.int_id_);
                assert(link_vals != null);
                
                // This portion is similar to Flow::make() in Sim

                // This portion is similar to Flow::find_pathway_with_link_allocation in Sim
                ArrayList<Pathway> potential_paths = new ArrayList<Pathway>();
                ArrayList<Pathway> completed_paths = new ArrayList<Pathway>();

                // Find all links in the network from the flow's source that have some bandwidth
                // availible and start paths from them.
                ArrayList<Link> links_to_remove = new ArrayList<Link>();
                for (Link l : link_vals) {
                    if (l.src_loc_ == f.src_loc_) {
                        Pathway p = new Pathway();
                        p.node_list_.add(l.src_loc_);
                        p.node_list_.add(l.dst_loc_);

                        if (l.dst_loc_ == f.dst_loc_) {
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
                            if (p.last_node() != l.src_loc_) {
                                continue;
                            }

                            // Does the bandwidth available on this link directly match the bandwidth
                            // of this pathway?
                            if (Math.round(Math.abs(p.bandwidth_ - l.cur_bw_) * 100.0) / 100.0 < 0.01) {
                                p.node_list_.add(l.dst_loc_);
                                links_to_remove.add(l);
                                link_added = true;
                                links_to_remove.add(l);

                                // Check if path is now complete
                                if (l.dst_loc_ == f.dst_loc_) {
                                    paths_to_remove.add(p);
                                    completed_paths.add(p);
                                }
                            }

                            // Does this link have less bandwidth than the bandwidth available on the path?
                            // Split the path in two -- one path taking this link (and reducing its bandwidth)
                            // and the other not taking the path and using the remaining bandwidth.
                            else if (Math.round((p.bandwidth_ - l.cur_bw_ * 100.0)) / 100.0 >= 0.01) {
                                Pathway new_p = new Pathway();
                                new_p.bandwidth_ = p.bandwidth_ - l.cur_bw_;
                                new_p.node_list_ = (ArrayList<String>)p.node_list_.clone();
                                p.bandwidth_ = l.cur_bw_;
                                p.node_list_.add(l.dst_loc_);
                                link_added = true;
                                links_to_remove.add(l);
                                
                                // Check if path is now complete
                                if (l.dst_loc_ == f.dst_loc_) {
                                    paths_to_remove.add(p);
                                    completed_paths.add(p);
                                }
                            }

                            // Does the link have more bandwidth than the bandwidth available on the path?
                            // Only reduce the link's bandwidth by the amount that could be used by the path.
                            else if (Math.round((p.bandwidth_ - l.cur_bw_ * 100.0)) / 100.0 <= -0.01) {
                                l.cur_bw_ = l.cur_bw_ - p.bandwidth_;
                                p.node_list_.add(l.dst_loc_);
                                link_added = true;
                                // Check if path is now complete
                                if (l.dst_loc_ == f.dst_loc_) {
                                    paths_to_remove.add(p);
                                    completed_paths.add(p);
                                }
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

                } // while link_vals

                f.paths_ = completed_paths;

























            }
        }
        /*for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);
            
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            System.exit(1);
        }*/
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
    
    public ArrayList<Map.Entry<Coflow, Double>> sort_coflows(HashMap<String, Coflow> coflows) throws Exception {
        HashMap<Coflow, Double> cct_map = new HashMap<Coflow, Double>();

        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            cct_map.put(c, mmcf_out.completion_time_);
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
