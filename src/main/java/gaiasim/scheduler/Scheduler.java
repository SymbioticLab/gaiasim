package gaiasim.scheduler;

import gaiasim.network.*;
import org.graphstream.graph.Edge;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Math.min;

public abstract class Scheduler {
    public NetGraph net_graph_;

    public int droppedCnt = 0;
    public int missDDLCNT = 0;

    // All possible links in our graph
    public SubscribedLink[][] links_;

    public Scheduler(NetGraph net_graph) {
        net_graph_ = net_graph;

        links_ = new SubscribedLink[net_graph_.nodes_.size() + 1][net_graph_.nodes_.size() + 1];
        for (Edge e : net_graph_.graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());
            links_[src][dst] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
            links_[dst][src] = new SubscribedLink(Double.parseDouble(e.getAttribute("bandwidth").toString()));
        }
    }

    public abstract void finish_flow(Flow f);

    public abstract double progress_flow(Flow f);

    public void reset_links() {
        for (int i = 0; i < net_graph_.nodes_.size() + 1; i++) {
            for (int j = 0; j < net_graph_.nodes_.size() + 1; j++) {
                if (links_[i][j] != null) {
                    links_[i][j].subscribers_.clear();
                }
            }
        }
    }

    public abstract HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows,
                                                         long timestamp) throws Exception;

    public abstract void update_flows(HashMap<String, Flow> flows);

    // Called when a new coflow is added; only used by the DarkScheduler for now.
    public void add_coflow(Coflow c) {
    }

    // Called when an existing coflow finishes; only used by the DarkScheduler for now.
    public void remove_coflow(Coflow c) {
    }

    boolean assignBWforFlow_MaxSinglePath(Flow flow, long timestamp) {

        if (flow.done_){
            return false;
        }

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

//            flows_.put(flow.id_, flow);
            if (flow.start_timestamp_ == -1) {
                flow.start_timestamp_ = timestamp;
            }

            flow.scheduled_alone = true;
            System.out.println("Flow " + flow.id_ + " allocated " + flow.paths_.get(0));

            return true;
        } else {
            return false;
        }
    }
}
