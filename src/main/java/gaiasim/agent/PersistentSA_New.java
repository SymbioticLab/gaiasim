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

// for now just copy the code and modify.

import gaiasim.comm.ControlMessage;
import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.ScheduleMessage;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class PersistentSA_New implements Runnable{

    // Inner class DataBroker, function:
    // 1. Upon construction, create persistentConn and send PA messages
    //
//    public class NewDataBroker extends DataBroker{

//        public NewDataBroker(String id, NetGraph net_graph, Socket sd) {
//            super(id , net_graph , sd);
//        }

    String id_;
    String trace_id_;
    NetGraph netGraph;
    Configuration config;

    final ScheduledExecutorService statusExec;


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
    public HashMap<String, PersistentConnection[]> connection_pools_ = new HashMap<String, PersistentConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
    public HashMap<String, PersistentConnection> connections_ = new HashMap<String, PersistentConnection>();

    // Flows that are currently being sent by this SendingAgent
    public HashMap<String, FlowInfo> flows_ = new HashMap<String, FlowInfo>();

    public Socket socketToController;
    public ObjectOutputStream os_;
    public ObjectInputStream is_;


    // TODO: use the new PA msg.
    public void sendPAMessages(NetGraph net_graph){
        for (String ra_id : net_graph.nodes_) {

            if (!id_.equals(ra_id)) { // don't consider path to SA itself.

                // because apap is consistent among different programs.
                PersistentConnection[] conns = new PersistentConnection[net_graph.apap_.get(id_).get(ra_id).size()];
                for (int i = 0; i < conns.length; i++) {
                    // ID of connection is SA_id-RA_id.path_id
                    String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);
                    int raID = Integer.parseInt(ra_id);

                    try {
                        // Create the socket that the PersistentConnection object will use
                        Socket socketToRA = new Socket( config.getRAIP(raID) , config.getRAPort(raID));
                        int port = socketToRA.getLocalPort();
                        PersistentConnection conn = new PersistentConnection(conn_id, socketToRA);
                        conns[i] = conn;
                        connections_.put(conn.data_.id_, conn);

                        // Inform the controller of the port number selected
                        writeMessage(new PortAnnouncementMessage_Old(id_, ra_id, i, port));
                    }
                    catch (java.io.IOException e) {
                        // TODO: Close socket
                        e.printStackTrace();
                        System.exit(1);
                    }
                }

                connection_pools_.put(ra_id, conns);

            } // if id != ra_id

        } // for ra_id in nodes
    }

    private void writeMessage(PortAnnouncementMessage_Old m) throws IOException {
        os_.writeObject(m);
    }

    public synchronized void writeMessage(ScheduleMessage m) throws java.io.IOException {
        System.out.println("Should not write this legacy ScheduleMessage");
    }

    // the new message interface we use
    public synchronized void writeMessage(AgentMessage m) throws java.io.IOException {
        os_.writeObject(m);
    }

    public synchronized void finish_flow(String flow_id) {
        // We don't send out ScheduleMessage this time. so that CTRL can decode correctly.
        // Think about flow_status Message. TODO check when this message is sent out.

        AgentMessage m = new FlowStatusMessage(flow_id,-1, true);
        try {
            writeMessage(m);
        } catch (IOException e) {
            e.printStackTrace();
        }

        flows_.remove(flow_id);
    }

    // overriden, call this method on every 100 ms.
    public synchronized void get_status() {
        try {
            int size = flows_.size();
            if(size == 0){
                return;
            }
            String [] fid = new String[size];
            double [] transmitted = new double[size];
            boolean [] isFinished = new boolean[size];
            int i = 0;
            for (String k : flows_.keySet()) {
                FlowInfo f = flows_.get(k);
                fid[i] = f.id_;
                transmitted[i] = f.transmitted_;
                isFinished[i] = f.done_;

                i++;
//                    f.set_update_pending(true); // no need?
            }
            AgentMessage m = new FlowStatusMessage(size , fid , transmitted , isFinished);
            writeMessage(m);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            // TODO: Close socket
            return;
        }
    }



    @Override
    public void run() {

        final Runnable sendStatus = this::get_status;
        ScheduledFuture<?> mainHandler = statusExec.scheduleAtFixedRate(sendStatus, 0, 200, MILLISECONDS);

        NewListener listener = new NewListener();
        listener.run();
    }


    // Listens for CTRL messages, and contains some event handling logic.
    private class NewListener implements Runnable {

        public NewListener() {}

        public void run() {
            try {
                while (true) {
                    ControlMessage c = (ControlMessage) is_.readObject();

                    // TODO change the message into the only ONE message that we will be using.

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(trace_id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(!flows_.containsKey(c.flow_id_));

                        FlowInfo f = new FlowInfo(c.flow_id_, c.field0_, c.field1_, dataBroker);
                        flows_.put(f.id_, f);
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
                        System.out.println(trace_id_ + " FLOW_UPDATE(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(flows_.containsKey(c.flow_id_));
                        FlowInfo f = flows_.get(c.flow_id_);
                        f.update_flow(c.field0_, c.field1_);
                    }
                    else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
                        System.out.println(trace_id_ + " SUBFLOW_INFO(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert flows_.containsKey(c.flow_id_) : trace_id_ + " does not currently have " + c.flow_id_;
                        FlowInfo f = flows_.get(c.flow_id_);
                        PersistentConnection conn = connection_pools_.get(c.ra_id_)[c.field0_];
                        f.add_subflow(conn, c.field1_);
                    }
                    else {
                        System.out.println(trace_id_ + " received an unexpected ControlMessage");
//                        System.exit(1);
                    }
                }
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
            catch (java.lang.ClassNotFoundException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
        }
    } // class Listener


    public PersistentSA_New(String id, NetGraph net_graph, Socket client_sd , Configuration configuration) {
        statusExec = Executors.newScheduledThreadPool(1);
        id_ = id;
        netGraph = net_graph;
        config = configuration;

        try {
            socketToController = client_sd;
            os_ = new ObjectOutputStream(socketToController.getOutputStream());
            is_ = new ObjectInputStream(socketToController.getInputStream());
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        trace_id_ = Constants.node_id_to_trace_id.get(id);
        sendPAMessages(netGraph);
    }
}
