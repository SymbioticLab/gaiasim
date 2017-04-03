package gaiasim.agent;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.Connection;
import gaiasim.agent.FlowInfo;
import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

public class SendingAgent {
   
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

        // DEBUG ONLY
        public LinkedBlockingQueue<ScheduleMessage> to_sac_queue_;
        public LinkedBlockingQueue<ControlMessage> from_sac_queue_;

        public Data(String id, NetGraph net_graph, 
                    LinkedBlockingQueue<ControlMessage> from_sac_queue,
                    LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {
            id_ = id;
            trace_id_ = Constants.node_id_to_trace_id.get(id);
            from_sac_queue_ = from_sac_queue;
            to_sac_queue_ = to_sac_queue;

            for (String dst : net_graph.nodes_) {

                if (!id_.equals(dst)) {

                    Connection[] conns = new Connection[net_graph.apap_.get(id_).get(dst).size()];
                    for (int i = 0; i < conns.length; i++) {
                        // TODO: - Create socket to dst and pass the socket to the new Connection
                        //       - Retrieve the socket's port (getsockopt) and send
                        //            <dst, i, port_no> back to controller

                        String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(dst) + "." + Integer.toString(i);
                        Connection conn = new Connection(conn_id);
                        conns[i] = conn;
                        connections_.put(conn.data_.id_, conn);
                    }

                    connection_pools_.put(dst, conns);

                } // if id_ != dst

            } // for dst in nodes
        }

        public synchronized void get_status() {
            try {
                for (String k : flows_.keySet()) {
                    FlowInfo f = flows_.get(k);
                    f.set_update_pending(true);
                    System.out.println("Sending STATUS_RESPONSE for " + f.id_ + " transmitted " + f.transmitted_ + " / " + f.volume_ + " and done=" + f.done_);
                    ScheduleMessage s = new ScheduleMessage(ScheduleMessage.Type.FLOW_STATUS_RESPONSE,
                                                            f.id_, f.transmitted_);
                    to_sac_queue_.put(s);
                }
            }
            catch (InterruptedException e) {
                // TODO: Close socket
                return;
            }
        }

        public synchronized void finish_flow(String flow_id) {
            ScheduleMessage s = new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_id);

            try {
                to_sac_queue_.put(s);
            }
            catch (InterruptedException e) {
                // TODO: Close socket
                return;
            }
            remove_flow(flow_id);
        }

        public synchronized void remove_flow(String flow_id) {
            flows_.remove(flow_id);
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
                    ControlMessage c = data_.from_sac_queue_.take();

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

        listen_ctrl_thread_ = new Thread(new Listener(data_));
        listen_ctrl_thread_.start();
    }
}
