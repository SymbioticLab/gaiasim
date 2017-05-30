package gaiasim.network;

import org.graphstream.algorithm.APSP;
import org.graphstream.algorithm.APSP.APSPInfo;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import gaiasim.util.Constants;

public class NetGraph {
    public Graph graph_;
    public ArrayList<String> nodes_ = new ArrayList<String>();
    public HashMap<String, String> trace_id_to_node_id_ = new HashMap<String, String>();

    // All pairs shortest path. First index is src node, second index
    // is dst node.
    public Path[][] apsp_;

    // All pairs most bandwidth (paths between each node which have the greatest
    // minimum link bandwidth).
    public Pathway[][] apmb_;

    // Max bandwidth of each link
    public Double[][] link_bw_;

    public NetGraph(String gml_file) throws java.io.IOException {
        graph_ = new SingleGraph("GaiaSimGraph");
        
        FileSource fs = FileSourceFactory.sourceFor(gml_file);
        fs.addSink(graph_);

        fs.readAll(gml_file);
        fs.removeSink(graph_);

        Constants.node_id_to_trace_id = new HashMap<String, String>();
        for (Node n:graph_) {
            nodes_.add(n.toString());
            trace_id_to_node_id_.put(n.getLabel("ui.label").toString(), n.toString());
            Constants.node_id_to_trace_id.put(n.toString(), n.getLabel("ui.label").toString());
        }

        link_bw_ = new Double[nodes_.size() + 1][nodes_.size() + 1];
        for (Edge e : graph_.getEachEdge()) {
            int src = Integer.parseInt(e.getNode0().toString());
            int dst = Integer.parseInt(e.getNode1().toString());

            // Use default bandwidth of 1 Gbps when it's unspecified
            if (e.getAttribute("bandwidth") == null) {
                e.setAttribute("bandwidth", "1024");
            }
            link_bw_[src][dst] = Double.parseDouble(e.getAttribute("bandwidth").toString());;
            link_bw_[dst][src] = Double.parseDouble(e.getAttribute("bandwidth").toString());;
        }
        
        APSP apsp = new APSP();
        apsp.init(graph_);
        apsp.setDirected(false);
        apsp.compute();
       
        // Since we'll be indexing this array by nodeID, and nodeID's start
        // at 1, we need num_nodes+1 entries in the array.
        apsp_ = new Path[nodes_.size() + 1][nodes_.size() + 1];
        apmb_ = new Pathway[nodes_.size() + 1][nodes_.size() + 1];

        for (Node n : graph_) {
            APSPInfo info = n.getAttribute(APSPInfo.ATTRIBUTE_NAME);

            for (Node n_ : graph_) {
                if (n.toString() != n_.toString()) {
                    int src = Integer.parseInt(n.toString());
                    int dst = Integer.parseInt(n_.toString());
                    apsp_[src][dst] = info.getShortestPathTo(n_.toString());

                    ArrayList<Pathway> paths = make_paths(n, n_);
                    Pathway max_bw_path = new Pathway();
                    for (Pathway p : paths) {
                        assign_bw(p);
                        if (p.bandwidth_ > max_bw_path.bandwidth_) {
                            max_bw_path = p;
                        }
                    }
                    
                    apmb_[src][dst] = max_bw_path;
                } 

            } // for n_

        } // for n
    }

    // Find the minimum link bandwidth available on p. This is the bandwidth available
    // on the pathway.
    private void assign_bw(Pathway p) {
        double min_bw = Double.MAX_VALUE;
        for (int i = 0; i < p.node_list_.size() - 1; i++) {
            int src = Integer.parseInt(p.node_list_.get(i));
            int dst = Integer.parseInt(p.node_list_.get(i+1));
            double bw = link_bw_[src][dst];

            if (bw < min_bw) {
                min_bw = bw;
            }
        }

        p.bandwidth_ = min_bw;
    }

    // Make all paths from src to dst
    private ArrayList<Pathway> make_paths(Node src, Node dst) {
        ArrayList<Pathway> pathways = new ArrayList<Pathway>();
        Pathway p = new Pathway();
        p.node_list_.add(src.toString());
        make_paths_helper(src, dst.toString(), p, pathways);
        return pathways;
    }

    // Perform depth-first-search to find all paths from cur to dst
    private void make_paths_helper(Node cur, String dst, Pathway cur_path, ArrayList<Pathway> pathways) {
        Iterator<Node> neighbor_it = cur.getNeighborNodeIterator();
        while (neighbor_it.hasNext()) {
            Node n = neighbor_it.next();
            String n_str = n.toString();
            if (!cur_path.node_list_.contains(n_str)) {
                cur_path.node_list_.add(n_str);
                if (dst.equals(n_str)) {
                    pathways.add(new Pathway(cur_path));
                }
                else {
                    make_paths_helper(n, dst, cur_path, pathways);
                }
                
                cur_path.node_list_.remove(n_str);

            } // if neighbor not in path

        } // while hasNext
    }

}
