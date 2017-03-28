package gaiasim.comm;

import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.SendingAgent;
import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;

// A SendingAgentContact facilitates communication between
// the GAIA controller and a sending agent at a certain datacenter.
public class SendingAgentContact {
    // ID of the sending agent with which this contact communicates
    public String id_;

    // Queue of messages between controller and SendingAgentContact.
    // The SendingAgentContact should only place messages on the queue.
    public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;

    // DEBUG ONLY
    public LinkedBlockingQueue<ControlMessage> to_sa_queue_ = new LinkedBlockingQueue<ControlMessage>();
    public LinkedBlockingQueue<ScheduleMessage> from_sa_queue_ = new LinkedBlockingQueue<ScheduleMessage>();
    public SendingAgent sa_;

    private class SendingAgentListener implements Runnable {
        public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;
        // TODO: Replace this with a socket
        public LinkedBlockingQueue<ScheduleMessage> from_sa_queue_;
        
        public SendingAgentListener(LinkedBlockingQueue<ScheduleMessage> schedule_queue, 
                                    LinkedBlockingQueue<ScheduleMessage> from_sa_queue) {
            schedule_queue_ = schedule_queue;
            from_sa_queue_ = from_sa_queue_;
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
                               LinkedBlockingQueue<ScheduleMessage> schedule_queue) {
        id_ = id;
        schedule_queue_ = schedule_queue;

        // TODO: Open connection with sending agent and
        //       get the port numbers that it plans to use
        sa_ = new SendingAgent(id_, net_graph, to_sa_queue_, from_sa_queue_);
        
        // Start thread to listen for messages from the
        // sending agent and add them to the queue.
        listen_sa_thread_ = new Thread(new SendingAgentListener(schedule_queue_, from_sa_queue_));
        listen_sa_thread_.start();
    }

    // Sends a FLOW_START or FLOW_UPDATE  message to the sending agent along
    // with information about the paths and rates used by the flow.
    public void startFlow(Flow f) {
        String start_or_update = f.updated_ ? "UPDATING" : "STARTING";
        System.out.println(start_or_update + " flow " + f.id_ + " at " + id_);
    }

    // Sends a message to the sending agent requesting an
    // update on the status of all active flows
    public void sendStatusRequest() {
        System.out.println("STATUS_REQUEST at " + id_);
    }

    public void terminate() {
        listen_sa_thread_.interrupt(); 
    }
}
