package gaiasim.scheduler;

// The scheduler for RAPIER

import gaiasim.mmcf.MMCFOptimizer;
import gaiasim.network.*;

import java.util.*;

import static java.lang.Math.min;

public class RapierScheduler extends BaselineScheduler {

    double waitingThreshold_ms = 100000; // default 100s

    public RapierScheduler(NetGraph net_graph) {
        super(net_graph);
    }

    public RapierScheduler(NetGraph net_graph, double waitingThreshold_ms) {
        super(net_graph);
        this.waitingThreshold_ms = waitingThreshold_ms;
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                long timestamp) throws Exception {
        flows_.clear();
        reset_links();

        // Order coflows based on CCT estimation
        ArrayList<Coflow> cf_list = sort_coflows_by_waitTime(coflows);

        ArrayList<Coflow> cf_to_remove = new ArrayList<>();
        ArrayList<Coflow> unscheduled_coflows = new ArrayList<>();

        while (cf_list.size() > 0) {


            double T_Min = Double.MAX_VALUE;
            Coflow cf_ToSchedule = null;

            for (Coflow cf : cf_list) {

                // get minCCT
                double T_C = minCCT(cf, net_graph_, links_);
                cf.minCCT = T_C;

                if (T_C == -1) {
                    unscheduled_coflows.add(cf);
                    cf_to_remove.add(cf);
                    continue;
                }

                // condition 1
                if (timestamp - cf.submitted_timestamp > waitingThreshold_ms) {
                    System.out.println("CF " + cf.id_ + " waited too long");
                    cf_ToSchedule = cf;
                    T_Min = T_C;

                    break;
                }

                // condition 2
                if (T_C < T_Min) {
                    T_Min = T_C;
                    cf_ToSchedule = cf;
                }

            }

            // remove CF from the arraylist
            cf_list.remove(cf_ToSchedule);
            cf_list.removeAll(cf_to_remove);
            cf_to_remove.clear();

            if (cf_ToSchedule == null) {
//                System.err.println("Can't find flow to schedule");
                break;
            }

            // Assign the rates
            for (Map.Entry<String, Flow> fe : cf_ToSchedule.flows_.entrySet()) {
                Flow f = fe.getValue();

                if (f.paths_.size() == 0) {
                    System.exit(-1);
                }

                for (Pathway p : f.paths_) {
                    for (int i = 0; i < p.node_list_.size() - 1; i++) {
                        int src = Integer.parseInt(p.node_list_.get(i));
                        int dst = Integer.parseInt(p.node_list_.get(i + 1));
                        links_[src][dst].subscribers_.add(p);
                    }
                }

                if (f.start_timestamp_ == -1) {
//                        System.out.println("Setting start_timestamp to " + timestamp);
                    f.start_timestamp_ = timestamp;
                }
                flows_.put(f.id_, f);
            }


/*            for ( Pair<Coflow, Double> pair : scheduled_cf_list){
                Coflow cf = pair.getKey();

                for ( Map.Entry<String, Flow> fe : cf.flows_.entrySet()) {
                    Flow f = fe.getValue();

                    for (Pathway p : f.paths_) {
                        for (int i = 0; i < p.node_list_.size() - 1; i++) {
                            int src = Integer.parseInt(p.node_list_.get(i));
                            int dst = Integer.parseInt(p.node_list_.get(i + 1));
                            links_[src][dst].subscribers_.add(p);
                        }
                    }

                    if (f.start_timestamp_ == -1) {
//                        System.out.println("Setting start_timestamp to " + timestamp);
                        f.start_timestamp_ = timestamp;
                    }

                    flows_.put(f.id_, f);
                }
            }*/


        }

        // Schedule any available flows one-by-one
        if (!unscheduled_coflows.isEmpty()) {
//            schedule_extra_flow_varys(unscheduled_coflows, timestamp);
            distributeBandwidth(unscheduled_coflows, timestamp);
        }

        // Now calculate rates
        update_flows(flows_);

        return flows_;

    }

    private double minCCT(Coflow cf, NetGraph net_graph_, SubscribedLink[][] links_) throws Exception {

        // First select a path for each flow (select the max B/W)
        // In the paper, this is done by a for loop for all the flows.

        // Simpler way for paths selection
        MMCFOptimizer.MMCFOutput mmcf_out = MMCFOptimizer.glpk_optimize(cf, net_graph_, links_, 1);
        if (mmcf_out.completion_time_ == -1.0) {
            return -1;
        }

        // check this coflow to see if fully scheduled
//        boolean all_flows_scheduled = true;
        for (String k : cf.flows_.keySet()) {
            Flow f = cf.flows_.get(k);

            if (f.done_) {
                if (f.remaining_volume() != 0) {
                    System.err.println("FATAL: remaining vol != 0");
                }
                continue; // ignoring this flow, continue to check other flows of this coflow
            }

            ArrayList<Link> link_vals = mmcf_out.flow_link_bw_map_.get(f.int_id_);

            // first phase: check if link exists
            if (link_vals == null || link_vals.size() == 0) {
                return -1;
/*                System.out.println("WARNING: no link is assigned by LP for flow " + f.id_);
                all_flows_scheduled = false;
                break; // break here, give up this coflow (clean up later)*/
            }

            // try to make paths
            PoorManScheduler.make_paths(f, link_vals);

            // check if we can actually make paths
            if (f.paths_.size() == 0) {
/*                System.out.println("WARNING: no paths is created in make_path() for flow " + f.id_);
                // make paths failed for this flow, we move the owning coflow into unscheduled later
                all_flows_scheduled = false;*/
                return -1;
//                break; // break here, give up this coflow (clean up later)
            }
        }

        // If we reach here, all flows must be scheduled, choose the max BW path for each flow

        for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()) {
            Flow f = fe.getValue();
            // select the path with the max B/W
            Pathway maxPath = selectMaxBWPath(f);

            if (maxPath == null) {
//                System.exit(-1);
                return -1;
            }

            f.paths_.clear();
            f.paths_.add(maxPath);

        }


        // Another way for paths selection
/*
        for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()) {
            Flow f = fe.getValue();

            // select the path with the max B/W
            Pathway maxPath = selectMaxBWPath(f, net_graph_, links_);

            if (maxPath == null) {
//                System.exit(-1);
                return -1;
            }

            f.paths_.clear();
            f.paths_.add(maxPath);

        }
*/


        // After setting the path for each flow, we don't actually need to run LP to find the minCCT
        // for each link we try to cap the A

        double A = Double.MAX_VALUE;
        for (int i = 0; i < net_graph_.nodes_.size() + 1; i++) {
            for (int j = 0; j < net_graph_.nodes_.size() + 1; j++) {
                if (links_[i][j] != null) {
                    // for each link, check all flows and the corresponding paths, and cap A

                    double cur_bw = links_[i][j].remaining_bw();

                    double sum_volume = 0;
                    for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()) {
                        Flow f = fe.getValue();

                        Pathway p = f.paths_.get(0);
                        // if the paths contains this link, add up the volume
                        if (p.containsLink(i, j)) {

                            sum_volume += f.remaining_volume();
                        }
                    }

                    if (sum_volume > 0) {
                        if (cur_bw > 0) {
                            // if there is a flow on this link, and the link has remaining BW
                            double A_cmp = cur_bw / sum_volume;
                            // Cap A
                            if (A > A_cmp) {
                                A = A_cmp;
                            }
                        } else {
                            System.err.println("ERR: no BW!");
                        }
                    }
                }
            }
        }

        // Update the path B/W after calculated A

        for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()) {
            Flow f = fe.getValue();

            assert (f.paths_.size() == 1);

            f.paths_.get(0).bandwidth_ = A * f.remaining_volume();
        }

        return 1.0E00 / A;

    }

    private Pathway selectMaxBWPath(Flow f) {
        Pathway maxPath = null;
        double maxBW = 0;
        for (Pathway p : f.paths_) {
            if (p.bandwidth_ > maxBW) {
                maxBW = p.bandwidth_;
                maxPath = p;
            }
        }
        return maxPath;
    }

/*
    private Pathway selectMaxBWPath(Flow flow, NetGraph net_graph_, SubscribedLink[][] links_) throws Exception {

        // DFS search

        // try use MaxFlowOptimizer instead

        Coflow tmpCF = new Coflow("SINGLE", null);

        tmpCF.volume_ = 0.0;

        tmpCF.volume_ += flow.volume_;
        tmpCF.flows_.put(flow.id_, flow);

        MaxFlowOptimizer.MaxFlowOutput mf_out = MaxFlowOptimizer.glpk_optimize(tmpCF, net_graph_, links_);
//        LoadBalanceOptimizer.LoadBalanceOutput mf_out = LoadBalanceOptimizer.glpk_optimize(combined_coflow, net_graph_, links_);


        for (Flow f : tmpCF.flows_.values()) {

            f.paths_.clear(); // first clear the paths.

            ArrayList<Link> link_vals = mf_out.flow_link_bw_map_.get(f.int_id_);

            if (link_vals != null) {
                PoorManScheduler.make_paths(f, link_vals);
            } else {
                return null;
//                System.err.println("link_val == null");
            }

            if (f.paths_.size() == 0) {
                return null;
//                System.err.println("ERROR! No Path found");
            }

            // find the paths with most B/W
            double maxBW = 0;
            Pathway maxP = null;
            for (Pathway p : f.paths_) {
                if (maxBW < p.bandwidth_) {
                    maxBW = p.bandwidth_;
                    maxP = p;
                }
            }
            return maxP;
        }

        System.err.println("No paths for flow " + flow.id_);
        return null;

    }*/

    private void distributeBandwidth(ArrayList<Coflow> unscheduled_coflows, long timestamp) {
        // first sort the Coflows according to the MinCCT
        // But they should all have minCCT == -1, so no need to sort!
/*        LinkedList<Coflow> CF_N1 = new LinkedList<>();
        LinkedList<Coflow> CF_N2 = new LinkedList<>();
        LinkedList<Coflow> CF_POSITIVE = new LinkedList<>();
        for (Coflow cf : unscheduled_coflows) {
            if (cf.minCCT == -1) {
                CF_N1.add(cf);
            } else if (cf.minCCT == -2) {
                CF_N2.add(cf);
            } else if (cf.minCCT > 0) {
                CF_POSITIVE.add(cf);
            } else {
                System.err.println("FATAL");
            }
        }

        // sort from large to small
        Collections.sort(CF_POSITIVE, (o1, o2) -> {
            if (o1.minCCT < o2.minCCT) {
                return 1;
            } else if (o1.minCCT > o2.minCCT) {
                return -1;
            } else return 0;
        });*/
        for (Coflow cf : unscheduled_coflows) {
            assignBWforCF(cf, timestamp);
        }

    }

    private void assignBWforCF(Coflow cf, long timestamp) {
        ArrayList<Flow> flows = new ArrayList<Flow>(cf.flows_.values());

        Collections.sort(flows, Comparator.comparingDouble(o -> o.remaining_volume()));
        // then from large flow to small flow
        for (int i = flows.size() - 1; i >= 0; i--) {
            assignBWforFlow(flows.get(i), timestamp);
        }
    }

    private void assignBWforFlow(Flow flow, long timestamp) {

        // use Dijkstra's algorithm to find the Max Bottleneck Path
        int num_nodes = net_graph_.nodes_.size();
        int src = Integer.parseInt(flow.src_loc_);
        int dst = Integer.parseInt(flow.dst_loc_);

        int cur_node = src;
        Set<Integer> visited_nodes = new HashSet<>();
        Set<Integer> unvisited_nodes = new HashSet<>();
        double BW[] = new double[num_nodes + 1];
        int prev[] = new int[num_nodes + 1];

        for (int i = 1; i <= num_nodes; i++) {
            BW[i] = 0; // TODO
            prev[i] = -1;
            unvisited_nodes.add(i);
        }

        BW[src] = Double.MAX_VALUE;

        while (!unvisited_nodes.isEmpty()) {
            // first find the node to evaluate
            double max_cur_BW = -100;
            for (int node : unvisited_nodes) {
                if (BW[node] > max_cur_BW) {
                    max_cur_BW = BW[node];
                    cur_node = node;
                }
            }

            unvisited_nodes.remove(cur_node);
            visited_nodes.add(cur_node);

            // for each neighbour from cur_node
            for (int j = 1; j <= num_nodes; j++) {
                if (links_[cur_node][j] != null) {
                    double alt = min(links_[cur_node][j].remaining_bw(), BW[cur_node]);
                    if (alt > BW[j]) {
                        BW[j] = alt;
                        prev[j] = cur_node;
                    }
                }
            }
        }

        // parse and assign the flow
        if (prev[dst] != -1) {
            Pathway p = new Pathway();
            int node = dst;
            while (node != src) {
                p.node_list_.add(String.valueOf(node));
                node = prev[node];
                assert (node > 0);
            }
            p.node_list_.add(String.valueOf(src));

            Collections.reverse(p.node_list_);
            p.bandwidth_ = BW[dst];

            flow.paths_.clear();
            flow.paths_.add(p);

            for (int i = 0; i < p.node_list_.size() - 1; i++) {
                int cur_src = Integer.parseInt(p.node_list_.get(i));
                int cur_dst = Integer.parseInt(p.node_list_.get(i + 1));
                links_[cur_src][cur_dst].subscribers_.add(p);
            }

            flows_.put(flow.id_, flow);
            if (flow.start_timestamp_ == -1) {
                flow.start_timestamp_ = timestamp;
            }
        }

    }

   /* private void schedule_extra_flow_varys(ArrayList<Coflow> unscheduled_coflows, long timestamp) {

        // Below is the code from Gaia
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
    }*/

    private ArrayList<Coflow> sort_coflows_by_waitTime(HashMap<String, Coflow> coflows) throws Exception {

        ArrayList<Coflow> cf_list = new ArrayList<>(coflows.values());
        // sort from small to large, so the waitTime long from short
        Collections.sort(cf_list, Comparator.comparingLong(o -> o.submitted_timestamp));

        return cf_list;
    }

    private boolean checkCF_FIN(Coflow c) {

        boolean isFIN = true;

        for (String k_ : c.flows_.keySet()) {
            Flow f = c.flows_.get(k_);
            if (f.done_) {
                continue;
            } else {
                isFIN = false;
            }
        }

        return isFIN;
    }
}
