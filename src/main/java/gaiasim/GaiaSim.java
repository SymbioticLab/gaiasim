package gaiasim;

import java.util.HashMap;

import gaiasim.gaiamaster.Master;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GaiaSim {
    public static boolean is_emulation_ = false;

    private static final Logger logger = LoggerFactory.getLogger(GaiaSim.class);

    public static HashMap<String, String> parse_cli(String[] args) 
                                                    throws org.apache.commons.cli.ParseException {

        HashMap<String, String> args_map = new HashMap<String, String>();
        Options options = new Options();
        options.addOption("g", true, "path to gml file");
        options.addOption("j", true, "path to trace file");
        options.addOption("s", true, "scheduler to use. One of {baseline, recursive-remain-flow}");
        options.addOption("o", true, "path to directory to save output files");
        options.addOption("e", false, "run under emulation");
        options.addOption("c", true, "path to config file");

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

        if (cmd.hasOption("o")) {
            args_map.put("outdir", cmd.getOptionValue("o"));
        }
        else {
            args_map.put("outdir", "/tmp");
        }

        if (cmd.hasOption("e")) {
            is_emulation_ = true;
        }

        if (cmd.hasOption("c")){
            args_map.put("config" , cmd.getOptionValue("c"));
        }
        else {
            args_map.put("config" , null);
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
            Process p = Runtime.getRuntime().exec("cp models/MinCCT.mod /tmp/MinCCT.mod");
            p.waitFor();

            logger.info("GAIA: finished copying the model..");

            Master m = new Master(args_map.get("gml"), args_map.get("trace"),
                                    args_map.get("scheduler"), args_map.get("outdir") , args_map.get("config"));

            if (is_emulation_) {
                m.emulate();
            }
            else {
                m.simulate();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        return;
    }
}
