package gaiasim.agent;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.Connection;
import gaiasim.agent.FlowInfo;
import gaiasim.comm.ControlMessage;
import gaiasim.comm.PortAnnouncementMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

// Maintains persistent connections with each receiving agent.
// Each path between the sending agent and receiving agent
// has its own persistent connection -- any time a flow is
// to traverse this path it will use this connection (NOTE: if
// this becomes a scalability concern one can maintain a pool
// of connections for each path rather than a single connection).
//
// Flows can be preempted by the controller. Preemption involves
// the controller sending a FLOW_STATUS_REQUEST message, after
// which the sending agent responds with the amount that has
// been transmitted by each active flow. The controller then sends
// back FLOW_UPDATE with its updated allocation for the flow. During
// the time between a FLOW_STATUS_REQUEST being received and the
// FLOW_UPDATE being received, the sending agent will continue
// to send the flow. So it may be the case that, by the time we
// receive a FLOW_UPDATE, we have already completed the flow.
public class PersistentSendingAgent {
   
    public class Data {
        String id_;
        String trace_id_;

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
        public HashMap<String, Connection[]> connection_pools_ = new HashMap<String, Connection[]>();

        // A Map of all Connections, indexed by Connection ID. Connection ID is
        // composed of ReceivingAgentID + PathID.
        public HashMap<String, Connection> connections_ = new HashMap<String, Connection>();

        // Flows that are currently being sent by this SendingAgent
        public HashMap<String, FlowInfo> flows_ = new HashMap<String, FlowInfo>();

        public Socket sd_;
        public ObjectOutputStream os_;
        public ObjectInputStream is_;

        public Data(String id, NetGraph net_graph, Socket sd) {
            id_ = id;

            try {
                sd_ = sd;
                os_ = new ObjectOutputStream(sd.getOutputStream());
                is_ = new ObjectInputStream(sd.getInputStream());
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

            trace_id_ = Constants.node_id_to_trace_id.get(id);

            for (String ra_id : net_graph.nodes_) {

                if (!id_.equals(ra_id)) {

                    Connection[] conns = new Connection[net_graph.apap_.get(id_).get(ra_id).size()];
                    for (int i = 0; i < conns.length; i++) {
                        // ID of connection is SA_id-RA_id.path_id
                        String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);

                        try {
                            // Create the socket that the Connection object will use
                            Socket conn_sd = new Socket("10.0.0." + (Integer.parseInt(ra_id) + 1), 33330);
                            int port = conn_sd.getLocalPort();
                            Connection conn = new Connection(conn_id, conn_sd);
                            conns[i] = conn;
                            connections_.put(conn.data_.id_, conn);

                            // Inform the controller of the port number selected
                            writeMessage(new PortAnnouncementMessage(id_, ra_id, i, port));
                        }
                        catch (java.io.IOException e) {
                            // TODO: Close socket
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }

                    connection_pools_.put(ra_id, conns);

                } // if id_ != ra_id

            } // for ra_id in nodes
        }

        public synchronized void get_status() {
            try {
                for (String k : flows_.keySet()) {
                    FlowInfo f = flows_.get(k);
                    f.set_update_pending(true);
                    System.out.println("Sending STATUS_RESPONSE for " + f.id_ + " transmitted " + f.transmitted_ + " / " + f.volume_ + " and done=" + f.done_);
                    ScheduleMessage s = new ScheduleMessage(ScheduleMessage.Type.FLOW_STATUS_RESPONSE,
                                                            f.id_, f.transmitted_);
                    writeMessage(s);
                }
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
        }

        public synchronized void finish_flow(String flow_id) {
            ScheduleMessage s = new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_id);

            try {
                writeMessage(s);
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
            flows_.remove(flow_id);
        }

        public synchronized void writeMessage(PortAnnouncementMessage m) throws java.io.IOException {
            os_.writeObject(m);
        }

        public synchronized void writeMessage(ScheduleMessage m) throws java.io.IOException {
            os_.writeObject(m);
        } 

    } // class Data

    private class Listener implements Runnable {
        public Data data_;
        
        public Listener(Data data) {
            data_ = data;
        }

        public void run() {
            try {
                while (true) {
                    ControlMessage c = (ControlMessage) data_.is_.readObject();

                    // TODO: Consider turning the functionality for FLOW_UPDATE and
                    //       SUBFLOW_INFO into their own synchronized functions to
                    //       avoid potential problems with a flow being finished in
                    //       the middle of while we're calling one of these.
                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(data_.trace_id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(!data_.flows_.containsKey(c.flow_id_));

                        FlowInfo f = new FlowInfo(c.flow_id_, c.field0_, c.field1_, data_);
                        data_.flows_.put(f.id_, f);
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
                        System.out.println(data_.trace_id_ + " FLOW_UPDATE(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(data_.flows_.containsKey(c.flow_id_));
                        FlowInfo f = data_.flows_.get(c.flow_id_);
                        f.update_flow(c.field0_, c.field1_);
                    }
                    else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
                        System.out.println(data_.trace_id_ + " SUBFLOW_INFO(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert data_.flows_.containsKey(c.flow_id_) : data_.trace_id_ + " does not currently have " + c.flow_id_;
                        FlowInfo f = data_.flows_.get(c.flow_id_);
                        Connection conn = data_.connection_pools_.get(c.ra_id_)[c.field0_];
                        f.add_subflow(conn, c.field1_);
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_STATUS_REQUEST) {
                        System.out.println(data_.trace_id_ + " FLOW_STATUS_REQUEST");
                        data_.get_status();
                    }
                    else if (c.type_ == ControlMessage.Type.TERMINATE) {
                        for (String k : data_.connections_.keySet()) {
                            Connection conn = data_.connections_.get(k);
                            conn.terminate();
                        }

                        // TODO: Close socket
                        return;
                    }
                    else {
                        System.out.println(data_.trace_id_ + " received an unexpected ControlMessage");
                        System.exit(1);
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
       
    public Data data_;
    public Thread listen_ctrl_thread_;

    public PersistentSendingAgent(String id, NetGraph net_graph, Socket client_sd) {
        data_ = new Data(id, net_graph, client_sd);

        listen_ctrl_thread_ = new Thread(new Listener(data_));
        listen_ctrl_thread_.start();
    }
}
