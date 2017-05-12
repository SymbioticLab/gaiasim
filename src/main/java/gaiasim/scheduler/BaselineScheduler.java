package gaiasim.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.network.*;
import gaiasim.network.FlowGroup;
import gaiasim.util.Constants;

public class BaselineScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, FlowGroup> flows_ = new HashMap<String, FlowGroup>();

    public BaselineScheduler(NetGraph net_graph) {
        super(net_graph);
    }
    
    public void finish_flow(FlowGroup f) {
        ArrayList<String> nodes = f.paths.get(0).node_list;
        for (int i = 0; i < nodes.size()- 1; i++) {
            int src = Integer.parseInt(nodes.get(i));
            int dst = Integer.parseInt(nodes.get(i+1));
            links_[src][dst].subscribers_.remove(f.paths.get(0));
        }
    }

    public void progress_flow(FlowGroup f) {
        f.setTransmitted_volume(f.getTransmitted_volume() + f.getRate() * Constants.SIMULATION_TIMESTEP_SEC);
    }

    public HashMap<String, FlowGroup> schedule_flows(HashMap<String, Coflow> coflows,
                                                     long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);

            for (String k_ : c.flows.keySet()) {
                FlowGroup f = c.flows.get(k_);
                if (f.isDone()) {
                    continue;
                }
                
                //Path gp = net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)];
                //Pathway p = new Pathway(gp);
                Pathway p = net_graph_.apmb_[Integer.parseInt(f.getSrc_loc())][Integer.parseInt(f.getDst_loc())];
                f.paths.clear();
                f.paths.add(p);
              
                for (int i = 0; i < p.node_list.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list.get(i));
                    int dst = Integer.parseInt(p.node_list.get(i+1));
                    links_[src][dst].subscribers_.addAll(f.paths);
                }

                if (f.getStart_timestamp() == -1) {
                    f.setStart_timestamp(timestamp);
                }

                flows_.put(f.getId(), f);
            }
        }
        
        for (String k : flows_.keySet()) {
            FlowGroup f = flows_.get(k);

            double min_bw = Double.MAX_VALUE;

            ArrayList<String> nodes = f.paths.get(0).node_list;
            for (int i = 0; i < nodes.size() - 1; i++) {
                int src = Integer.parseInt(nodes.get(i));
                int dst = Integer.parseInt(nodes.get(i+1));
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }
            f.setRate(min_bw);
//            f.paths_.get(0).bandwidth = min_bw;
            f.paths.get(0).setBandwidth( min_bw);
            System.out.println("FlowGroup " + f.getId() + " has rate " + f.getRate() + " and remaining volume " + (f.getVolume() - f.getTransmitted_volume()) + " on path " + f.paths.get(0));
        }

        return flows_;
    }

    // Updates the rates of flows
    public void update_flows(HashMap<String, FlowGroup> flows) {
        for (String k : flows.keySet()) {
            FlowGroup f = flows.get(k);

            double min_bw = Double.MAX_VALUE;

            ArrayList<String> nodes = f.paths.get(0).node_list;
            for (int i = 0; i < nodes.size() - 1; i++) {
                int src = Integer.parseInt(nodes.get(i));
                int dst = Integer.parseInt(nodes.get(i+1));
                double link_bw = links_[src][dst].bw_per_flow();

                if (link_bw < min_bw) {
                    min_bw = link_bw;
                }
            }
            f.setRate(min_bw);
            System.out.println("FlowGroup " + f.getId() + " has rate " + f.getRate() + " and remaining volume " + (f.getVolume() - f.getTransmitted_volume()) + " on path " + f.paths.get(0));
        }
    }
}
