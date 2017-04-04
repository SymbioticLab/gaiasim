package gaiasim.agent;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.util.Constants;

// Acts on behalf of the controller to start transfers
// from one node to another. Does not keep any persistent
// connections between nodes -- every flow gets its own
// socket and traverses the topology selected by the normal
// routing strategy. Once started, flows are not preempted
// by the controller.
public class BaselineSendingAgent {
    
    public class Data {
        public String id_;
        public String trace_id_;

        // DEBUG ONLY
        public LinkedBlockingQueue<ScheduleMessage> to_sac_queue_;
        public LinkedBlockingQueue<ControlMessage> from_sac_queue_;
 
        public Data(String id, 
                    LinkedBlockingQueue<ControlMessage> from_sac_queue,   
                    LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {
            id_ = id;
            trace_id_ = Constants.node_id_to_trace_id.get(id);

            to_sac_queue_ = to_sac_queue;
            from_sac_queue_ = from_sac_queue;
        }
    }

    // A simple class to maintain info about an in-progress flow
    public class LightFlow {
        public String id_;
        public double volume_;
        public double transmitted_ = 0;

        public LightFlow(String id, double volume) {
            id_ = id;
            volume_ = volume;
        }
    }

    public class Sender implements Runnable {
        public LightFlow flow_;
        public String ra_ip_; // IP address of receiving agent

        // DEBUG ONLY
        public LinkedBlockingQueue<ScheduleMessage> to_sac_queue_;

        public Sender(String flow_id, double volume, String ra_ip, LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {
            flow_ = new LightFlow(flow_id, volume);
            ra_ip_ = ra_ip;
            to_sac_queue_ = to_sac_queue;
        }

        public void run() {
            // TODO: Create socket to ra_ip
            Random rnd = new Random(System.currentTimeMillis());

            while (flow_.transmitted_ < flow_.volume_) {
                try {
                    Thread.sleep(rnd.nextInt(1000));
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                flow_.transmitted_ += 1024; 
            }

            // TODO: Close socket

            try {
                to_sac_queue_.put(new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_.id_));
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Listens for messages from the controller and dispatches threads
    // to send flows.
    public class Listener implements Runnable {
        public Data data_;
        
        public Listener(Data d) {
            data_ = d;
        }

        public void run() {
            try {
                while (true) {
                    ControlMessage c = data_.from_sac_queue_.take();

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(data_.trace_id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        // TODO: Create thread to start flow
                        (new Thread(new Sender(c.flow_id_, c.field1_, "JACK DO THIS", data_.to_sac_queue_))).start();
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
                        System.out.println("ERROR: Received FLOW_UPDATE for baseline scheduler");
                        System.exit(1);
                    }
                    else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
                        System.out.println("ERROR: Received SUBFLOW_INFO for baseline scheduler");
                        System.exit(1);
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_STATUS_REQUEST) {
                        System.out.println("ERROR: Received FLOW_STATUS_REQUEST for baseline scheduler");
                        System.exit(1);
                    }
                    else if (c.type_ == ControlMessage.Type.TERMINATE) {
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
    }

    public Data data_;
    public Thread listener_;

    public BaselineSendingAgent(String id, 
            LinkedBlockingQueue<ControlMessage> from_sac_queue,   
            LinkedBlockingQueue<ScheduleMessage> to_sac_queue) {

        data_ = new Data(id, from_sac_queue, to_sac_queue);
    
        listener_ = new Thread(new Listener(data_));
        listener_.start();
    }


}
