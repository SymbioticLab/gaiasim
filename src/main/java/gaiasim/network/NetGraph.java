package gaiasim.network;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceFactory;

import java.util.ArrayList;

public class NetGraph {
    public Graph graph_;
    public ArrayList<String> nodes_ = new ArrayList<String>();

    public NetGraph(String gml_file) throws java.io.IOException {
        graph_ = new SingleGraph("GaiaSimGraph");
        
        FileSource fs = FileSourceFactory.sourceFor(gml_file);
        fs.addSink(graph_);

        fs.readAll(gml_file);
        fs.removeSink(graph_);

        for (Node n:graph_) {
            nodes_.add(n.getLabel("ui.label").toString());
        }
    }
}
