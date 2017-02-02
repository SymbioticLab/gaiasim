package gaiasim.network;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceFactory;

public class NetGraph {
    public Graph graph;

    public NetGraph(String gml_file) throws java.io.IOException {
        graph = new SingleGraph("GaiaSimGraph");
        
        FileSource fs = FileSourceFactory.sourceFor(gml_file);
        fs.addSink(graph);

        fs.readAll(gml_file);
        fs.removeSink(graph);

        System.out.println("Nodes: ");
        for (Node n:graph) {
            System.out.println(n.getAttribute("ui.label"));
        }
    }
}
