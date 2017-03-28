package gaiasim.agent;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.Connection;
import gaiasim.agent.FlowInfo;
import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.NetGraph;

public class SendingAgent {
   
    private class Data {
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
        public HashMap<String, Connection[]> connection_pools_ = new HashMap<String, Connection[]>();

        // A Map of all Connections, indexed by Connection ID. Connection ID is
        // composed of ReceivingAgentID + PathID.
        public HashMap<String, Connection> connections_ = new HashMap<String, Connection>();

        // Flows that are currently being sent by this SendingAgent
        public HashMap<String, FlowInfo> flows_ = new HashMap<String, FlowInfo>();

        // DEBUG ONLY
        public LinkedBlockingQueue<ScheduleMessage> to_sac_queue_;
        public LinkedBlockingQueue<ControlMessage> from_sac_queue_;

        public Data(String id, NetGraph net_graph, 
                    LinkedBlockingQueue<ControlMessage> from_sac_queue,
                    LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {
            id_ = id;
            from_sac_queue_ = from_sac_queue;
            to_sac_queue_ = to_sac_queue;

            for (String dst : net_graph.nodes_) {

                if (!id_.equals(dst)) {

                    Connection[] conns = new Connection[net_graph.apap_.get(id_).get(dst).size()];
                    for (int i = 0; i < conns.length; i++) {
                        // TODO: - Create socket to dst and pass the socket to the new Connection
                        //       - Retrieve the socket's port (getsockopt) and send
                        //            <dst, i, port_no> back to controller

                        Connection conn = new Connection(dst + Integer.toString(i));
                        conns[i] = conn;
                        connections_.put(conn.data_.id_, conn);
                    }

                    connection_pools_.put(dst, conns);

                } // if id_ != dst

            } // for dst in nodes
        }

    } // class Data

    private class Listener implements Runnable {
        public Data data_;
        
        public Listener(Data data) {
            data_ = data;
        }

        public void run() {
            try {
                ControlMessage c = data_.from_sac_queue_.take();
                
                if (c.type_ == ControlMessage.Type.FLOW_START) {
                }
                else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
                }
                else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
                }
                else if (c.type_ == ControlMessage.Type.FLOW_STATUS_REQUEST) {
                    for (String k : data_.flows_.keySet()) {
                        FlowInfo f = data_.flows_.get(k);
                        ScheduleMessage s = new ScheduleMessage(ScheduleMessage.Type.FLOW_STATUS_RESPONSE,
                                                                f.id_, "JACK DEAL WITH THIS",
                                                                f.transmitted_);
                        data_.to_sac_queue_.put(s);
                    }
                }
            }
            catch (InterruptedException e) {
                // TODO: Close socket
                return;
            }
        }
    } // class Listener
       
    public Data data_;
    public Thread listen_ctrl_thread_;

    public SendingAgent(String id, NetGraph net_graph, 
                        LinkedBlockingQueue<ControlMessage> from_sac_queue,
                        LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {
        data_ = new Data(id, net_graph, from_sac_queue, to_sac_queue);

        // TODO: Start a thread to listen for messages from the Controller
        listen_ctrl_thread_ = new Thread(new Listener(data_));
        listen_ctrl_thread_.start();
    }
}
