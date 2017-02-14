package gaiasim.network;

import java.util.ArrayList;
import java.util.List;

import org.graphstream.graph.*;

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

    // Returns the last node in the node_list_
    public String last_node() {
        return node_list_.get(node_list_.size() - 1);
    }
}
