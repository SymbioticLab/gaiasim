package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.gaiaagent.PersistentSA_New;
import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SendingAgent_Old {

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

        // TODO add option for whether use persistent connection.
        //        options.addOption("")

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

//        String id = args[0];
//        String use_persistent = args[1];

        ServerSocket listener = new ServerSocket(config.getSAPort(Integer.parseInt(id)));  // always listening
        listener.setSoTimeout(0);

        NetGraph net_graph = new NetGraph(gmlFilePath ,1); // sending agent is unaware of the bw_factor

        Socket socketToCTRL = listener.accept();
        socketToCTRL.setSoTimeout(0);
        socketToCTRL.setTcpNoDelay(true);
        socketToCTRL.setKeepAlive(true);
        logger.info("SA: Accepted socket from CTRL. Starting RRF.");

        PersistentSA_New p = new PersistentSA_New(id, net_graph, socketToCTRL , config);
        p.run();

    }
}
