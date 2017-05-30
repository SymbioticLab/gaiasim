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

        for (Node n : graph_) {
            APSPInfo info = n.getAttribute(APSPInfo.ATTRIBUTE_NAME);

            for (Node n_ : graph_) {
                if (n.toString() != n_.toString()) {
                    int src = Integer.parseInt(n.toString());
                    int dst = Integer.parseInt(n_.toString());
                    apsp_[src][dst] = info.getShortestPathTo(n_.toString());
                }

            } // for n_

        } // for n
    }
}
