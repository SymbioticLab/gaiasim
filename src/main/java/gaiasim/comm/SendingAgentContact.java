package gaiasim.comm;

import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.Flow;

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
    public LinkedBlockingQueue<ControlMessage> from_sa_queue_ = new LinkedBlockingQueue<ControlMessage>();

    public SendingAgentContact(String id, String sa_ip, String sa_port, 
                               LinkedBlockingQueue<ScheduleMessage> schedule_queue) {
        id_ = id;
        schedule_queue_ = schedule_queue;

        // TODO: Open connection with sending agent and
        //       get the port numbers that it plans to use
        
        // TODO: Start thread to listen for messages from the
        // sending agent and add them to the queue.
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
}
