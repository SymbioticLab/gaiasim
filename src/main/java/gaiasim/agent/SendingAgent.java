package gaiasim.agent;

import gaiasim.agent.BaselineSendingAgent;
import gaiasim.agent.PersistentSendingAgent;
import gaiasim.network.NetGraph;

public class SendingAgent {
    public static void main(String[] args) throws java.io.IOException {
        if (args.length > 4) {
            System.out.println("ERROR: Incorrect number of command line arguments");
            System.out.println("  Usage: java -cp target/<jar-name> gaiasim.agent.SendingAgent <sa_id> <use_persistent_conn> <path_to_gml_file>");
            System.out.println("    sa_id: id of sending agent");
            System.out.println("    use_persistent_conn: 0 for new connection for each flow (baseline), 1 for using persistent connections between sending and receiving agents");
            System.out.println("    path_to_gml_file: path to gml file for emulated topology (only used if use_persistent_conn is 1");
            System.exit(1);
        }

        String id = args[1];
        String use_persistent = args[2];
        if (use_persistent.equals("0")) {
            //BaselineSendingAgent b = new BaselineSendingAgent(id);
        }
        else {
            String gml_file = args[3];
            NetGraph net_graph = new NetGraph(gml_file);
            //PersistentSendingAgent p = new PersistentSendingAgent(id, net_graph);
        }
    }
}
