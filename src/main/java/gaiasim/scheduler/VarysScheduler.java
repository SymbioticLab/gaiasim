package gaiasim.scheduler;

import gaiasim.mmcf.MMCFOptimizer;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;

import java.util.*;

public class VarysScheduler extends BaselineScheduler {
    public VarysScheduler(NetGraph net_graph) {
        super(net_graph);
    }

    private boolean fully_schedulable(Coflow c) {
        boolean no_overlap = true;

        for (String k_ : c.flows_.keySet()) {
            Flow f = c.flows_.get(k_);
            if (f.done_) {
                continue;
            }

            Pathway p = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
            f.paths_.clear();

            for (int i = 0; i < p.node_list_.size() - 1; i++) {
                int src = Integer.parseInt(p.node_list_.get(i));
                int dst = Integer.parseInt(p.node_list_.get(i + 1));
                if (!links_[src][dst].subscribers_.isEmpty()) {
                    no_overlap = false;
                    break;
                }
            }
        }

        return no_overlap;
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        // Order coflows based on CCT estimation
        ArrayList<Map.Entry<Coflow, Double>> cct_list = sort_coflows(coflows);
        ArrayList<Coflow> unscheduled_coflows = new ArrayList<Coflow>();

        for (Map.Entry<Coflow, Double> e : cct_list) {
            Coflow c = e.getKey();

            // Ignore coflow that cannot be scheduled in its entirety
            if (!fully_schedulable(c)) {
                unscheduled_coflows.add(c);
                continue;
            }

            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);
                if (f.done_) {
                    continue;
                }

                Pathway p = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
                f.paths_.clear();
                f.paths_.add(p);

                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i + 1));
                    links_[src][dst].subscribers_.addAll(f.paths_);
                }

                if (f.start_timestamp_ == -1) {
                    f.start_timestamp_ = timestamp;
                }

                flows_.put(f.id_, f);
            }
        }

        // Schedule any available flows one-by-one
        if (!unscheduled_coflows.isEmpty()) {
            schedule_extra_flows(unscheduled_coflows, timestamp);
        }

        // Now calculate rates
        update_flows(flows_);

        return flows_;
    }

    private void schedule_extra_flows(ArrayList<Coflow> unscheduled_coflows, long timestamp) {
        for (Coflow c : unscheduled_coflows) {
            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);
                if (f.done_) {
                    continue;
                }

                Pathway p = new Pathway(net_graph_.apsp_[Integer.parseInt(f.src_loc_)][Integer.parseInt(f.dst_loc_)]);
                f.paths_.clear();
                f.paths_.add(p);

                boolean no_overlap = true;
                for (int i = 0; i < p.node_list_.size() - 1; i++) {
                    int src = Integer.parseInt(p.node_list_.get(i));
                    int dst = Integer.parseInt(p.node_list_.get(i + 1));
                    if (!links_[src][dst].subscribers_.isEmpty()) {
                        no_overlap = false;
                        break;
                    }
                }

                if (no_overlap) {
                    for (int i = 0; i < p.node_list_.size() - 1; i++) {
                        int src = Integer.parseInt(p.node_list_.get(i));
                        int dst = Integer.parseInt(p.node_list_.get(i + 1));
                        links_[src][dst].subscribers_.addAll(f.paths_);
                    }

                    if (f.start_timestamp_ == -1) {
                        f.start_timestamp_ = timestamp;
                    }

                    flows_.put(f.id_, f);
                }
            }
        }
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
}
