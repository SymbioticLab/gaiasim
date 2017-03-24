package gaiasim.comm;

import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.ScheduleMessage;

// A SendingAgentContact facilitates communication between
// the GAIA controller and a sending agent at a certain datacenter.
public class SendingAgentContact {

    // Queue of messages between controller and SendingAgentContact.
    // The SendingAgentContact should only place messages on the queue.
    public LinkedBlockingQueue<ScheduleMessage> message_queue_;

    public SendingAgentContact(String sa_ip, String sa_port, 
                               LinkedBlockingQueue<ScheduleMessage> message_queue) {
        message_queue_ = message_queue;

        // TODO: Open connection with sending agent and
        //       get the port numbers that it plans to use
        
        // TODO: Start thread to listen for messages from the
        // sending agent and add them to the queue.
    }

    // Sends a flow start message to the sending agent along
    // with information about the paths and rates used by
    // the flow.
    public void addFlow() {

    }

    // Sends a message to the sending agent requesting an
    // update on the status of all active flows
    public void sendStatus() {

    }
}
