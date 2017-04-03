package gaiasim.comm;

import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.BaselineSendingAgent;
import gaiasim.agent.PersistentSendingAgent;
import gaiasim.comm.ControlMessage;
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

    // DEBUG ONLY
    public LinkedBlockingQueue<ControlMessage> to_sa_queue_ = new LinkedBlockingQueue<ControlMessage>();
    public LinkedBlockingQueue<ScheduleMessage> from_sa_queue_ = new LinkedBlockingQueue<ScheduleMessage>();
    public PersistentSendingAgent sa_;
    public BaselineSendingAgent bsa_;

    private class SendingAgentListener implements Runnable {
        public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;
        // TODO: Replace this with a socket
        public LinkedBlockingQueue<ScheduleMessage> from_sa_queue_;
        
        public SendingAgentListener(LinkedBlockingQueue<ScheduleMessage> schedule_queue, 
                                    LinkedBlockingQueue<ScheduleMessage> from_sa_queue) {
            schedule_queue_ = schedule_queue;
            from_sa_queue_ = from_sa_queue;
        }
         
        public void run() {
            while (true) {
                try {
                    ScheduleMessage s = from_sa_queue_.take();
                    schedule_queue_.put(s);
                }
                catch (InterruptedException e) {
                    // TODO: Close socket with SA
                    return;
                }
            }
        }
    }

    public Thread listen_sa_thread_;

    public SendingAgentContact(String id, NetGraph net_graph, String sa_ip, String sa_port, 
                               LinkedBlockingQueue<ScheduleMessage> schedule_queue,
                               boolean is_baseline) {
        id_ = id;
        net_graph_ = net_graph;
        schedule_queue_ = schedule_queue;
        is_baseline_ = is_baseline;

        // TODO: Open connection with sending agent and
        //       get the port numbers that it plans to use
        if (is_baseline) {
            bsa_ = new BaselineSendingAgent(id_, to_sa_queue_, from_sa_queue_);
        }
        else {
            sa_ = new PersistentSendingAgent(id_, net_graph, to_sa_queue_, from_sa_queue_);
        }
        
        // Start thread to listen for messages from the
        // sending agent and add them to the queue.
        listen_sa_thread_ = new Thread(new SendingAgentListener(schedule_queue_, from_sa_queue_));
        listen_sa_thread_.start();
    }

    // Sends a FLOW_START or FLOW_UPDATE  message to the sending agent along
    // with information about the paths and rates used by the flow.
    public void startFlow(Flow f) {
        String start_or_update = f.updated_ ? "UPDATING" : "STARTING";
        System.out.println(start_or_update + " flow " + f.id_ + " at " + Constants.node_id_to_trace_id.get(id_));

        ControlMessage c = new ControlMessage();
        c.type_ = f.updated_ ? ControlMessage.Type.FLOW_UPDATE : ControlMessage.Type.FLOW_START;
        c.flow_id_ = f.id_;
        c.field0_ = f.paths_.size();
        c.field1_ = f.remaining_volume();
        
        try {
            to_sa_queue_.put(c);

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

                    to_sa_queue_.put(sub_c);
                }
            }
        }
        catch (InterruptedException e) {
            // TODO: Close socket
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Sends a message to the sending agent requesting an
    // update on the status of all active flows
    public void sendStatusRequest() {
        System.out.println("STATUS_REQUEST at " + id_);
        
        ControlMessage c = new ControlMessage();
        c.type_ = ControlMessage.Type.FLOW_STATUS_REQUEST;

        try {
            to_sa_queue_.put(c);   
        }
        catch (InterruptedException e) {
            // TODO: Close socket
            System.exit(1);
        }
    }

    public void terminate() {
        listen_sa_thread_.interrupt();

        try {
            to_sa_queue_.put(new ControlMessage(ControlMessage.Type.TERMINATE));
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
