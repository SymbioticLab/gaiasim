package gaiasim.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;

import gaiasim.mmcf.MMCFOptimizer;
import gaiasim.network.*;
import gaiasim.network.FlowGroup_Old;
import gaiasim.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PoorManScheduler extends Scheduler {

    private static final Logger logger = LogManager.getLogger();

    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, FlowGroup_Old> flows_ = new HashMap<String, FlowGroup_Old>();

    public PoorManScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(FlowGroup_Old f) {
        for (Pathway p : f.paths) {
            for (int i = 0; i < p.node_list.size() - 1; i++) {
                int src = Integer.parseInt(p.node_list.get(i));
                int dst = Integer.parseInt(p.node_list.get(i+1));
                links_[src][dst].subscribers_.remove(p);
            }
        }
    }

    public void make_paths(FlowGroup_Old f, ArrayList<Link> link_vals) {
        // TODO: Consider just choosing the shortest path (measured by hops)
        //       from src to dst if the flow has volume below some threshold.
        //       See if not accounting for bw consumption on a certain link
        //       makes any affect.

        // This portion is similar to FlowGroup::find_pathway_with_link_allocation in Sim
        ArrayList<Pathway> potential_paths = new ArrayList<Pathway>();
        ArrayList<Pathway> completed_paths = new ArrayList<Pathway>();

        // Find all links in the network from the flow's source that have some bandwidth
        // availible and start paths from them.
        ArrayList<Link> links_to_remove = new ArrayList<Link>();
        for (Link l : link_vals) {
            if (l.src_loc_.equals(f.getSrc_loc())) {
                Pathway p = new Pathway();
                p.node_list.add(l.src_loc_);
                p.node_list.add(l.dst_loc_);
//                p.bandwidth = l.cur_bw_;
                p.setBandwidth( l.cur_bw_);

                if (l.dst_loc_.equals(f.getDst_loc())) {
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
                    if (Math.round(Math.abs(p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 < 0.01) {
                        p.node_list.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does this link have less bandwidth than the bandwidth available on the path?
                    // Split the path in two -- one path taking this link (and reducing its bandwidth)
                    // and the other not taking the path and using the remaining bandwidth.
                    else if (Math.round((p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 >= 0.01) {
                        Pathway new_p = new Pathway();
//                        new_p.bandwidth = p.getBandwidth() - l.cur_bw_;
                        new_p.setBandwidth( p.getBandwidth() - l.cur_bw_) ;
                        new_p.node_list = (ArrayList<String>)p.node_list.clone();
                        potential_paths.add(new_p);
//                        p.bandwidth = l.cur_bw_;
                        p.setBandwidth( l.cur_bw_ );
                        p.node_list.add(l.dst_loc_);
                        link_added = true;

                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
                            paths_to_remove.add(p);
                            completed_paths.add(p);
                        }

                        links_to_remove.add(l);
                        break;
                    }

                    // Does the link have more bandwidth than the bandwidth available on the path?
                    // Only reduce the link's bandwidth by the amount that could be used by the path.
                    else if (Math.round((p.getBandwidth() - l.cur_bw_) * 100.0) / 100.0 <= -0.01) {
                        l.cur_bw_ = l.cur_bw_ - p.getBandwidth();
                        p.node_list.add(l.dst_loc_);
                        link_added = true;
                        // Check if path is now complete
                        if (l.dst_loc_.equals(f.getDst_loc())) {
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
        f.paths.clear();
        f.paths = completed_paths;
    }

    public void progress_flow(FlowGroup_Old f) {
        for (Pathway p : f.paths) {
            f.setTransmitted_volume(f.getTransmitted_volume() + p.getBandwidth() * Constants.SIMULATION_TIMESTEP_SEC);
        }
    }

    public double remaining_bw() {
        double remaining_bw = 0.0;
        for (int i = 0; i < net_graph_.nodes_.size(); i++) {
            for (int j = 0; j < net_graph_.nodes_.size(); j++) {
                if (links_[i][j] != null) {
                    remaining_bw += links_[i][j].remaining_bw();
                }
            }
        }

        return remaining_bw;
    }

    public void schedule_extra_flows(ArrayList<Coflow_Old> unscheduled_coflows, long timestamp) {
        ArrayList<FlowGroup_Old> unscheduled_flowGroups = new ArrayList<FlowGroup_Old>();
        for (Coflow_Old c : unscheduled_coflows) {
            for (String k : c.flows.keySet()) {
                FlowGroup_Old f = c.flows.get(k);
                if (f.remaining_volume() > 0) {
                    unscheduled_flowGroups.add(c.flows.get(k));
                }
            }
        }
        Collections.sort(unscheduled_flowGroups, new Comparator<FlowGroup_Old>() {
            public int compare(FlowGroup_Old o1, FlowGroup_Old o2) {
                if (o1.getVolume() == o2.getVolume()) return 0;
                return o1.getVolume() < o2.getVolume() ? -1 : 1;
            }
        });

        for (FlowGroup_Old f : unscheduled_flowGroups) {
            int src = Integer.parseInt(f.getSrc_loc());
            int dst = Integer.parseInt(f.getDst_loc());
            Pathway p = new Pathway(net_graph_.apsp_[src][dst]);

            double min_bw = Double.MAX_VALUE;
            SubscribedLink[] path_links = new SubscribedLink[p.node_list.size() - 1];
            for (int i = 0; i < p.node_list.size() - 1; i++) {
                int lsrc = Integer.parseInt(p.node_list.get(i));
                int ldst = Integer.parseInt(p.node_list.get(i+1));
                SubscribedLink l = links_[lsrc][ldst];

                double bw = l.remaining_bw();
                path_links[i] = l;
                if (bw < min_bw) {
                    min_bw = bw;
                }
            }

            if (min_bw > 0) {
//                p.bandwidth = min_bw;
                p.setBandwidth( min_bw);

                for (SubscribedLink l : path_links) {
                    l.subscribers_.add(p);
                }
                f.paths.clear();
                f.paths.add(p);

/*                System.out.println("Adding separate flow " + f.getId() + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway path : f.paths) {
                    System.out.println("    " + path.toString());
                }*/

                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }
                flows_.put(f.getId(), f);
            }
        }
    }

    public HashMap<String, FlowGroup_Old> schedule_flows(HashMap<String, Coflow_Old> coflows,
                                                         long timestamp) throws Exception {
        flows_.clear();
        reset_links();
        ArrayList<Map.Entry<Coflow_Old, Double>> cct_list = sort_coflows(coflows); // here LP is called n times to sort.
        ArrayList<Coflow_Old> unscheduled_coflows = new ArrayList<Coflow_Old>();
        for (Map.Entry<Coflow_Old, Double> e : cct_list) {
            Coflow_Old c = e.getKey();

            if (remaining_bw() <= 0) {
                unscheduled_coflows.add(c);
                continue;
            }

//            System.out.println("Scheduler: Coflow " + c.getId() + " expected to complete in " + e.getValue() + " seconds");
            logger.info("Scheduler: Coflow {} expected to complete in {} seconds" ,c.getId() , e.getValue());

            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_); // This is the recursive part.

            boolean all_flows_scheduled = true;
            for (String k : c.flows.keySet()) {
                FlowGroup_Old f = c.flows.get(k);
                if (!f.isDone()) {
                    if (mmcf_out.flow_link_bw_map_.get(f.getInt_id()) == null) {
                        all_flows_scheduled = false;
                    }
                }
            }

            if (mmcf_out.completion_time_ == -1.0 || !all_flows_scheduled) {
                unscheduled_coflows.add(c);
                continue;
            }
            
            // This portion is similar to CoFlow::make() in Sim
            for (String k : c.flows.keySet()) {
                FlowGroup_Old f = c.flows.get(k);
                if (f.isDone()) {
                    continue;
                }

                ArrayList<Link> link_vals = mmcf_out.flow_link_bw_map_.get(f.getInt_id());
                assert(link_vals != null);

                // This portion is similar to FlowGroup::make() in Sim
                make_paths(f, link_vals);
                
                // Subscribe the flow's paths to the links it uses
                for (Pathway p : f.paths) {
                    for (int i = 0; i < p.node_list.size() - 1; i++) {
                        int src = Integer.parseInt(p.node_list.get(i));
                        int dst = Integer.parseInt(p.node_list.get(i+1));
                        links_[src][dst].subscribers_.add(p);
                    }
                }
                
/*                System.out.println("Adding flow " + f.getId() + " remaining = " + f.remaining_volume());
                System.out.println("  has pathways: ");
                for (Pathway p : f.paths) {
                    System.out.println("    " + p.toString());
                }*/

                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }

                flows_.put(f.getId(), f);
            }
        }

        // Schedule any available flows
        if (!unscheduled_coflows.isEmpty() && remaining_bw() > 0.0) {
            schedule_extra_flows(unscheduled_coflows, timestamp);        
        }
        return flows_;
    }
    
    public ArrayList<Map.Entry<Coflow_Old, Double>> sort_coflows(HashMap<String, Coflow_Old> coflows) throws Exception {
        HashMap<Coflow_Old, Double> cct_map = new HashMap<Coflow_Old, Double>();

        for (String k : coflows.keySet()) {
            Coflow_Old c = coflows.get(k);
            MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(c, net_graph_, links_);
            if (mmcf_out.completion_time_ != -1.0) {
                cct_map.put(c, mmcf_out.completion_time_);
            }
        }

        ArrayList<Map.Entry<Coflow_Old, Double>> cct_list = new ArrayList<Map.Entry<Coflow_Old, Double>>(cct_map.entrySet());
        Collections.sort(cct_list, new Comparator<Map.Entry<Coflow_Old, Double>>() {
            public int compare(Map.Entry<Coflow_Old, Double> o1, Map.Entry<Coflow_Old, Double> o2) {
                if (o1.getValue() == o2.getValue()) return 0;
                return o1.getValue() < o2.getValue() ? -1 : 1;
            }
        });

        return cct_list;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, FlowGroup_Old> flows) {}
}
