package gaiasim;

import java.util.HashMap;

import gaiasim.network.NetGraph;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;

public class GaiaSim {
    public static void main(String[] args) {
        System.out.println("Hello, world!");

        try {
            NetGraph ng = new NetGraph("/Users/jackkosaian/research/gaia/Sim/data/gml/swan.gml");
            HashMap <String, Job> jobs = DAGReader.read_trace("/Users/jackkosaian/research/gaia/Sim/data/job/simple_trace.txt", ng);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        
        return;
    }
}
