package gaiasim;

import java.util.HashMap;

import gaiasim.manager.Manager;
import gaiasim.network.NetGraph;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

public class GaiaSim {

    public static HashMap<String, String> parse_cli(String[] args) 
                                                    throws org.apache.commons.cli.ParseException {

        HashMap<String, String> args_map = new HashMap<String, String>();
        Options options = new Options();
        options.addOption("g", true, "path to gml file");
        options.addOption("j", true, "path to trace file");
        options.addOption("s", true, "scheduler to use. One of {baseline, recursive-remain-flow}");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        
        if (cmd.hasOption("g")) {
            args_map.put("gml", cmd.getOptionValue("g"));
        }
        else {
            System.out.println("ERROR: Must specify a path to a gml file using the -g flag");
            System.exit(1);
        }

        if (cmd.hasOption("j")) {
            args_map.put("trace", cmd.getOptionValue("j"));
        }
        else {
            System.out.println("ERROR: Must specify a path to a trace file using the -j flag");
            System.exit(1);
        }

        if (cmd.hasOption("s")) {
            args_map.put("scheduler", cmd.getOptionValue("s"));
        }
        else {
            System.out.println("ERROR: Must specify a scheduler {baseline, recursive-remain-flow} using the -s flag");
            System.exit(1);
        }

        return args_map;
    }

    public static void main(String[] args) {
        HashMap<String, String> args_map = null;
        try {
            args_map = parse_cli(args);
        }
        catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
            return;
        }

        try {
            Manager m = new Manager(args_map.get("gml"), args_map.get("trace"), args_map.get("scheduler"));
            m.simulate();
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        
        return;
    }
}
