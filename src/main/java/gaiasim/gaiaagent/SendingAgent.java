package gaiasim.gaiaagent;

// New sending agent using grpc.

import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@SuppressWarnings("Duplicates")

public class SendingAgent {
    private static final Logger logger = LogManager.getLogger();
    protected static Configuration config;

    public static void main(String[] args) throws java.io.IOException {
        Options options = new Options();
        options.addRequiredOption("g", "gml",true, "path to gml file");
        options.addRequiredOption("i", "id" ,true, "ID of this sending agent");
        options.addOption("c", "config",true, "path to config file");
        options.addOption("n" , "total-number" , true , "total number of agents");

        String id = null;
        String configfilePath;
        String gmlFilePath = null;

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();

        if(args.length == 0) {
            formatter.printHelp( "SendingAgent -i [id] -g [gml] -c [config]", options );
            return;
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("i")){
                id = cmd.getOptionValue('i');
            }

            if (cmd.hasOption("c")){
                configfilePath = cmd.getOptionValue('c');
                logger.info("using configuration from " + configfilePath);
                config = new Configuration(configfilePath);
            }
            else {
                if (cmd.hasOption('n')){
                    logger.info("using default configuration.");
                    config = new Configuration(Integer.parseInt(cmd.getOptionValue('n')));
                }
                else {
                    logger.error("Can't use default configuration without -n option.");
                    System.exit(1);
                }
            }

            if (cmd.hasOption("g")){
                gmlFilePath = cmd.getOptionValue('g');
                logger.info("using gml from file: " + gmlFilePath);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        NetGraph net_graph = new NetGraph(gmlFilePath ,1); // sending agent is unaware of the bw_factor

        // there should be 3 RPC servers?
        final AgentRPCServer server = new AgentRPCServer(id, net_graph, config);
        server.start();
        try {
            server.blockUntilShutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
