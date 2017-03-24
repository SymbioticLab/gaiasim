package gaiasim.network;

import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;

import org.graphstream.graph.*;

import gaiasim.util.Constants;

public class Pathway {
    public ArrayList<String> node_list_ = new ArrayList<String>();
    public double bandwidth_ = 0.0;

    public Pathway() {}

    public Pathway(Path p) {
        List<Node> path_nodes = p.getNodePath();
        for (Node n : path_nodes) {
            node_list_.add(n.toString());
        }
    }

    public Pathway(Pathway p) {
        bandwidth_ = p.bandwidth_;
        node_list_ = new ArrayList<String>(p.node_list_);
    }

    // Returns the last node in the node_list_
    public String last_node() {
        return node_list_.get(node_list_.size() - 1);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("[ ");
        for (String s : node_list_) {
            str.append(Constants.node_id_to_trace_id.get(s) + ", ");
        }
        str.append("] " + bandwidth_);
        return str.toString();
    }
}