package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.agent.BaselineSendingAgent;
import gaiasim.agent.PersistentSendingAgent;
import gaiasim.network.NetGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendingAgent {

    private final static Logger logger = LoggerFactory.getLogger(SendingAgent.class);
//    private final static Logger logger = LoggerFactory.getLogger(SendingAgent.class);

    public static void main(String[] args) throws java.io.IOException {
        if (args.length > 3) {
            System.out.println("ERROR: Incorrect number of command line arguments");
            System.out.println("  Usage: java -cp target/<jar-name> gaiasim.agent.SendingAgent <sa_id> <use_persistent_conn> <path_to_gml_file>");
            System.out.println("    sa_id: id of sending agent");
            System.out.println("    use_persistent_conn: 0 for new connection for each flow (baseline), 1 for using persistent connections between sending and receiving agents");
            System.out.println("    path_to_gml_file: path to gml file for emulated topology (only used if use_persistent_conn is 1");
            System.exit(1);
        }

        String id = args[0];
        String use_persistent = args[1];
        ServerSocket listener = new ServerSocket(23330);  // always listening

        System.setProperty("org.slf4j.simpleLogger.logFile" , "System.out"); // redirecting to stdout.





        try {

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
                    PersistentSendingAgent p = new PersistentSendingAgent(id, net_graph, socketToCTRL);
                }
            }

        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
