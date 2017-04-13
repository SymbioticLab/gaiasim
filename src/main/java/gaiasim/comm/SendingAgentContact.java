package gaiasim.comm;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.PersistentSendingAgent;
import gaiasim.comm.ControlMessage;
import gaiasim.comm.PortAnnouncementMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.util.Constants;

// A SendingAgentContact facilitates communication between
// the GAIA controller and a sending agent at a certain datacenter.
public class SendingAgentContact {
    // ID of the sending agent with which this contact communicates
    public String id_;

    // Queue of messages between controller and SendingAgentContact.
    // The SendingAgentContact should only place messages on the queue.
    public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;

    public NetGraph net_graph_;

    public boolean is_baseline_;

    public Socket sd_;
    public ObjectOutputStream os_;

    private class SendingAgentListener implements Runnable {
        public Socket sd_; // Connection to SendingAgent
        public ObjectInputStream is_;
        public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;
        public LinkedBlockingQueue<PortAnnouncementMessage> port_announce_queue_;
        public int num_port_announcements_;
        
        public SendingAgentListener(Socket sd,
                                    LinkedBlockingQueue<ScheduleMessage> schedule_queue,
                                    LinkedBlockingQueue<PortAnnouncementMessage> port_announce_queue,
                                    int num_port_announcements) {
            sd_ = sd;
            schedule_queue_ = schedule_queue;
            port_announce_queue_ = port_announce_queue;
            num_port_announcements_ = num_port_announcements;

            try {
                is_ = new ObjectInputStream(sd_.getInputStream());
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        
        // Receive messages from the SendingAgent (via socket) and relay them to the
        // controller (via queues). First receive all PortAnnouncements needed by
        // the controller to set up routes for this SendingAgent's paths. Then
        // receive update messages from the SendingaAgents
        public void run() {
            int num_ports_recv = 0;
            try {
                while (num_ports_recv < num_port_announcements_) {
                    PortAnnouncementMessage m = (PortAnnouncementMessage) is_.readObject();
                    port_announce_queue_.put(m);
                    num_ports_recv++;
                    System.out.println(id_ + " received " + num_ports_recv + " / " + num_port_announcements_);
                }
                while (true) {
                    ScheduleMessage s = (ScheduleMessage) is_.readObject();
                    schedule_queue_.put(s);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                // TODO: Close socket
                System.exit(1);
            }
        }
    }

    public Thread listen_sa_thread_;

    public SendingAgentContact(String id, NetGraph net_graph, String sa_ip, int sa_port, 
                               LinkedBlockingQueue<ScheduleMessage> schedule_queue,
                               LinkedBlockingQueue<PortAnnouncementMessage> port_announce_queue,
                               boolean is_baseline) {
        id_ = id;
        net_graph_ = net_graph;
        schedule_queue_ = schedule_queue;
        is_baseline_ = is_baseline;

        // Determine the number of port announcements that should be
        // received from the SendingAgent.
        int num_port_announcements = 0;
        for (String ra_id : net_graph_.apap_.get(id_).keySet()) {
            num_port_announcements += net_graph_.apap_.get(id_).get(ra_id).size();
        }
        System.out.println(id_ + " needs " + num_port_announcements);

        // Open connection with sending agent and
        // get the port numbers that it plans to use
        try {
            sd_ = new Socket(sa_ip, sa_port);
            os_ = new ObjectOutputStream(sd_.getOutputStream());
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        // Start thread to listen for messages from the
        // sending agent and add them to the queue.
        listen_sa_thread_ = new Thread(new SendingAgentListener(sd_, schedule_queue_, port_announce_queue, 
                                                                num_port_announcements));
        listen_sa_thread_.start();
    }

    // Sends a FLOW_START or FLOW_UPDATE  message to the sending agent along
    // with information about the paths and rates used by the flow.
    public void start_flow(Flow f) {
        String start_or_update = f.updated_ ? "UPDATING" : "STARTING";
        System.out.println(start_or_update + " flow " + f.id_ + " at " + Constants.node_id_to_trace_id.get(id_));

        ControlMessage c = new ControlMessage();
        c.type_ = f.updated_ ? ControlMessage.Type.FLOW_UPDATE : ControlMessage.Type.FLOW_START;
        c.flow_id_ = f.id_;
        c.field0_ = f.paths_.size();
        c.field1_ = f.remaining_volume();
        
        try {
            os_.writeObject(c);

            // Only send subflow info if running baseline
            if (!is_baseline_) {
                // Send information about each subflow of the flow
                for (Pathway p : f.paths_) {
                    ControlMessage sub_c = new ControlMessage();
                    sub_c.type_ = ControlMessage.Type.SUBFLOW_INFO;
                    sub_c.flow_id_ = f.id_;
                    sub_c.ra_id_ = p.dst();
                    sub_c.field0_ = net_graph_.get_path_id(p); // TODO: Store this with the path to reduce repeated call
                    sub_c.field1_ = p.bandwidth_;

                    os_.writeObject(sub_c);
                }
            }
        }
        catch (java.io.IOException e) {
            // TODO: Close socket
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Sends a message to the sending agent requesting an
    // update on the status of all active flows
    public void send_status_request() {
        System.out.println("STATUS_REQUEST at " + id_);
        
        ControlMessage c = new ControlMessage();
        c.type_ = ControlMessage.Type.FLOW_STATUS_REQUEST;

        try {
            os_.writeObject(c);
        }
        catch (java.io.IOException e) {
            // TODO: Close socket
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void terminate() {
        listen_sa_thread_.interrupt();

        try {
            os_.writeObject(new ControlMessage(ControlMessage.Type.TERMINATE));
            sd_.close();
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}
