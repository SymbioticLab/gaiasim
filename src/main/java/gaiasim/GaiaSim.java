package gaiasim;

import gaiasim.JCTCalc.JCTCalc;
import gaiasim.JCTCalc.UtilCalc;
import gaiasim.manager.Manager;
import gaiasim.scheduler.PoorManScheduler;
import gaiasim.util.Constants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.util.HashMap;

public class GaiaSim {

    private static HashMap<String, String> parse_cli(String[] args)
            throws org.apache.commons.cli.ParseException {

        HashMap<String, String> args_map = new HashMap<>();
        Options options = new Options();
        options.addOption("g", true, "path to gml file");
        options.addOption("j", true, "path to trace file");
        options.addOption("s", true, "scheduler to use. One of {baseline, recursive-remain-flow}");
        options.addOption("o", true, "path to directory to save output files");
        options.addOption("b", true, "scaling factor for bandwidth");
        options.addOption("w", true, "scaling factor for workload");
        options.addOption("k", true, "number of paths limited");
        options.addOption("onebyone", false, "insert the job one by one");
        options.addOption("nc", true, "numbers of computers in data center");
        options.addOption("csv", true, "input CCT csv file");
        options.addOption("util", false, "utilization calculation mode");
        options.addOption("ws", true, "window_start (for util)");
        options.addOption("we", true, "window_end (for util)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("g")) {
            args_map.put("gml", cmd.getOptionValue("g"));
        } else {
            System.out.println("ERROR: Must specify a path to a gml file using the -g flag");
            System.exit(1);
        }

        if (cmd.hasOption("j")) {
            args_map.put("trace", cmd.getOptionValue("j"));
        } else {
            System.out.println("ERROR: Must specify a path to a trace file using the -j flag");
            System.exit(1);
        }

        if (cmd.hasOption("o")) {
            args_map.put("outdir", cmd.getOptionValue("o"));
        } else {
            args_map.put("outdir", "/tmp");
        }

        // for util calculation
        double ws = 500;
        double we = 2000;

        if (cmd.hasOption("ws")) {
            ws = Double.parseDouble(cmd.getOptionValue("ws"));
        }

        if (cmd.hasOption("we")) {
            we = Double.parseDouble(cmd.getOptionValue("we"));
        }

        if (cmd.hasOption("nc")) {
            if (cmd.hasOption("csv")) {

                System.out.println("Choose JCT caculation mode");
                // parse the csv file

                JCTCalc jc = new JCTCalc(args_map.get("gml"), args_map.get("trace"), args_map.get("outdir"),
                        cmd.getOptionValue("csv"), cmd.getOptionValue("nc"));

                jc.calc();

                System.exit(0);

            }
        } else if (cmd.hasOption("util")) {
            if (cmd.hasOption("csv")) {

                System.out.println("Choose Utilization caculation mode");
                // parse the csv file

                UtilCalc uc = new UtilCalc(args_map.get("gml"), args_map.get("trace"), ws, we, cmd.getOptionValue("csv"));

                uc.calc();

                System.exit(0);

            }
        }

        System.out.println("Choose normal simualtion mode");

        if (cmd.hasOption("s")) {
            args_map.put("scheduler", cmd.getOptionValue("s"));
        } else {
            System.out.println("ERROR: Must specify a scheduler {baseline, recursive-remain-flow} using the -s flag");
            System.exit(1);
        }

        if (cmd.hasOption("b")) {
            args_map.put("bw_factor", cmd.getOptionValue("b"));
        } else {
            args_map.put("bw_factor", "1.0");
        }

        if (cmd.hasOption("w")) {
            args_map.put("workload_factor", cmd.getOptionValue("w"));
        } else {
            args_map.put("workload_factor", "1.0");
        }

        // Option k only valid for GAIA
        if (cmd.hasOption("k")) {
            PoorManScheduler.MAX_PARALLEL_PATHWAYS = Integer.parseInt(cmd.getOptionValue("k"));
            System.out.println("Path number limited to " + PoorManScheduler.MAX_PARALLEL_PATHWAYS);
        } else {
            PoorManScheduler.MAX_PARALLEL_PATHWAYS = Constants.DEFAULT_MAX_PARALLEL_PATHWAYS;
            System.out.println("Using default path number limit: " + Constants.DEFAULT_MAX_PARALLEL_PATHWAYS);
        }

        boolean isOneByOne = cmd.hasOption("onebyone");
        args_map.put("is_one_by_one", String.valueOf(isOneByOne));

        return args_map;
    }

    public static void main(String[] args) {
        HashMap<String, String> args_map;
        try {
            args_map = parse_cli(args);
        } catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
            return;
        }

        try {
            Process p = Runtime.getRuntime().exec("cp models/MinCCT.mod /tmp/MinCCT.mod");
            p.waitFor();
            p = Runtime.getRuntime().exec("cp models/LoadBalance.mod /tmp/LoadBalance.mod");
            p.waitFor();
            p = Runtime.getRuntime().exec("cp models/MaxFlow.mod /tmp/MaxFlow.mod");
            p.waitFor();

            Manager m = new Manager(args_map.get("gml"), args_map.get("trace"),
                    args_map.get("scheduler"), args_map.get("outdir"),
                    Double.parseDouble(args_map.get("bw_factor")),
                    Double.parseDouble(args_map.get("workload_factor")),
                    Boolean.parseBoolean(args_map.get("is_one_by_one")));
            m.simulate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
