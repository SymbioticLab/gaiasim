package gaiasim.agent;

import java.util.HashMap;

public class SendingAgent {
    String id_;
    
    // A Map containing Connections for each path from this SendingAgent to each
    // ReceivingAgent. The first index of the Map is the ReceivingAgent ID. The
    // second index (the index into the array of Connections) is the ID of the
    // path from the SendingAgent to the ReceivingAgent that is used by that
    // Connection.
    // TODO: Don't just have a single Connection per path, but a pool of
    //       Connections per path. This could be implemented as a LinkedBlockingQueue
    //       that Connections get cycled through in RR fashion. Or, if one really
    //       wanted to get fancy, Connections could be kept in some order based
    //       on their relative "hottness" -- how warmed up the TCP connection is.
    HashMap<String, Connection[]> connection_pools_ = new HashMap<String, Connection[]>();

    // A Map of all Connections, indexed by Connection ID. Connection ID is
    // composed of ReceivingAgentID + PathID.
    HashMap<String, Connection> connections_ = new HashMap<String, Connection>();
    
    public SendingAgent(String id) {
        id_ = id;
    }
}
