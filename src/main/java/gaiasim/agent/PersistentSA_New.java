package gaiasim.agent;

// The new persistentSendingAgent
// share the API as the old PersistentSendingAgent


// Maintains persistent connections with each receiving agent.
// Each path between the sending agent and receiving agent
// has its own persistent connection -- any time a flow is
// to traverse this path it will use this connection (NOTE: if
// this becomes a scalability concern one can maintain a pool
// of connections for each path rather than a single connection).


// function during normal operation
// (1) send heartbeat message of FLOW_STATUS to CTRL (what about finish message, do we send right away?)
// (2) receive and process FLOW_UPDATE from CTRL
// (3) set proper rate for persistent connections

// Function during bootstrap:
// (1) set up persistent connections.
// (2) Upon accepting the socket from CTRL, send PA messages.


// How do we implement throttled sending?
// How do we implement heartbeat message?
// What about thread safety?



import gaiasim.network.NetGraph;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

public class PersistentSA_New {

    String id;
    String trace_id;

    // A Map containing Connections for each path from this SendingAgent to each
    // ReceivingAgent. The first index of the Map is the ReceivingAgent ID. The
    // second index (the index into the array of Connections) is the ID of the
    // path from the SendingAgent to the ReceivingAgent that is used by that
    // PersistentConnection.
    // TODO: Don't just have a single PersistentConnection per path, but a pool of
    //       Connections per path. This could be implemented as a LinkedBlockingQueue
    //       that Connections get cycled through in RR fashion. Or, if one really
    //       wanted to get fancy, Connections could be kept in some order based
    //       on their relative "hottness" -- how warmed up the TCP connection is.

    // Map of ra_id  ->  list[]
    public HashMap<String, PersistentConnection[]> connectionPool = new HashMap<String, PersistentConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
    public HashMap<String, PersistentConnection> connections = new HashMap<String, PersistentConnection>();

    // Flows that are currently being sent by this SendingAgent
    public HashMap<String, FlowInfo> flows = new HashMap<String, FlowInfo>();

    public Socket socketToCTRL;
    public ObjectOutputStream objectOutputStream;
    public ObjectInputStream objectInputStream;


    public PersistentSA_New(String id, NetGraph net_graph, Socket client_sd) {

    }
}
