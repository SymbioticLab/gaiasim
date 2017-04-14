package gaiasim.agent;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
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
        public Socket sd_;
        public ObjectOutputStream os_;
        public ObjectInputStream is_;

        public Data(String id, Socket sd) {
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
        }

        public synchronized void writeMessage(ScheduleMessage m) throws java.io.IOException {
            os_.writeObject(m);
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
        public Data data_;
        public LightFlow flow_;
        public String ra_ip_; // IP address of receiving agent
        public Socket sd_;
        public OutputStream os_;
        public int buffer_size_ = 1024*1024;
        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
        public byte[] buffer_ = new byte[buffer_size_];

        public Sender(Data data, String flow_id, double volume, String ra_ip) {
            data_ = data;
            flow_ = new LightFlow(flow_id, volume);
            ra_ip_ = ra_ip;

            try {
                sd_ = new Socket(ra_ip, 33330);
                os_ = sd_.getOutputStream();
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void run() {
            while (flow_.transmitted_ < flow_.volume_) {
                try {
                    os_.write(buffer_);
                }
                catch (java.io.IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                // We track how much we've sent in terms of megabits
                flow_.transmitted_ += (buffer_size_megabits_); 
            }

            try {
                data_.writeMessage(new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_.id_));
                sd_.close();
            }
            catch (java.io.IOException e) {
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
                    ControlMessage c = (ControlMessage) data_.is_.readObject();

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(data_.id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        (new Thread(new Sender(data_, c.flow_id_, c.field1_, "10.0.0." + c.ra_id_))).start();
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
                        System.out.println(data_.id_ + " received an unexpected ControlMessage");
                        System.exit(1);
                    }
                }
            }
            catch (java.io.IOException e) {
                // TODO: Close socket
                return;
            }
            catch (java.lang.ClassNotFoundException e) {
                // TODO: Close socket
                return;
            }

        }
    }

    public Data data_;
    public Thread listener_;

    public BaselineSendingAgent(String id, Socket client_sd) {

        data_ = new Data(id, client_sd);
        listener_ = new Thread(new Listener(data_));
        listener_.start();
    }


}
