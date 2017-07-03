package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.gaiaagent.PersistentSA_New;
import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

/*        if (args.length > 3) {
            System.out.println("ERROR: Incorrect number of command line arguments");
            System.out.println("  Usage: java -cp target/<jar-name> gaiasim.agent.SendingAgent <sa_id> <use_persistent_conn> <path_to_gml_file>");
            System.out.println("    sa_id: id of sending agent");
            System.out.println("    use_persistent_conn: 0 for new connection for each flow (baseline), 1 for using persistent connections between sending and receiving agents");
            System.out.println("    path_to_gml_file: path to gml file for emulated topology (only used if use_persistent_conn is 1");
            System.exit(1);
        }*/

//        String id = args[0];
//        String use_persistent = args[1];

        ServerSocket listener = new ServerSocket(config.getSAPort(Integer.parseInt(id)));  // always listening

        NetGraph net_graph = new NetGraph(gmlFilePath ,1); // sending agent is unaware of the bw_factor

        Socket socketToCTRL = listener.accept();
        logger.info("SA: Accepted socket from CTRL. Starting RRF.");

        PersistentSA_New p = new PersistentSA_New(id, net_graph, socketToCTRL , config);
        p.run();


//        System.out.println("SA: Starting RRF.");




//                    PersistentSendingAgent p = new PersistentSendingAgent(id, net_graph, socketToCTRL);

//        System.setProperty("org.slf4j.simpleLogger.logFile" , "System.out"); // redirecting to stdout.


        // TODO need to support non-persistent sending agent.



/*        try {

            if (use_persistent.equals("0")) {
                while (true) {
                    Socket socketToCTRL = listener.accept(); //TODO: need to make *Agent() blocking. so we only serve one CTRL at a time.
                    logger.info("SA: Starting Baseline.");
                    BaselineSendingAgent b = new BaselineSendingAgent(id, socketToCTRL);
                }
            } else {
                String gml_file = args[2];
                NetGraph net_graph = new NetGraph(gml_file);
                while (true) {
                    Socket socketToCTRL = listener.accept();
                    logger.info("SA: Starting RRF.");
//                    PersistentSendingAgent p = new PersistentSendingAgent(id, net_graph, socketToCTRL);
                    PersistentSA_New p = new PersistentSA_New(id, net_graph, socketToCTRL);
                }
            }

        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

    }
}
