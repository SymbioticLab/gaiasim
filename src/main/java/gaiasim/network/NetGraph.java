package gaiasim.network;

import org.graphstream.algorithm.APSP;
import org.graphstream.algorithm.APSP.APSPInfo;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class NetGraph {
    public Graph graph_;
    public ArrayList<String> nodes_ = new ArrayList<String>();
    public HashMap<String, String> trace_id_to_node_id_ = new HashMap<String, String>();

    // All pairs shortest path. First index is src node, second index
    // is dst node.
    public Path[][] apsp_;

    public NetGraph(String gml_file) throws java.io.IOException {
        graph_ = new SingleGraph("GaiaSimGraph");
        
        FileSource fs = FileSourceFactory.sourceFor(gml_file);
        fs.addSink(graph_);

        fs.readAll(gml_file);
        fs.removeSink(graph_);

        for (Node n:graph_) {
            nodes_.add(n.toString());
            trace_id_to_node_id_.put(n.getLabel("ui.label").toString(), n.toString());
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
                    apsp_[Integer.parseInt(n.toString())][Integer.parseInt(n_.toString())] 
                        = info.getShortestPathTo(n_.toString());
                }
            }
        }
    }
}
