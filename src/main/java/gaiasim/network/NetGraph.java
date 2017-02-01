package gaiasim.network;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

public class NetGraph {
    public Graph graph;

    public NetGraph(String gml_file) {
        graph = new SingleGraph("GaiaSimGraph");
    }
}
