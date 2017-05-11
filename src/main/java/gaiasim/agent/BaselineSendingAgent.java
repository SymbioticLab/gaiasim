package gaiasim.agent;

import java.io.*;
import java.net.Socket;

import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.util.Constants;
import gaiasim.util.ThrottledOutputStream;

// Acts on behalf of the controller to start transfers
// from one node to another. Does not keep any persistent
// connections between nodes -- every flow gets its own
// socket and traverses the topology selected by the normal
// routing strategy. Once started, flows are not preempted
// by the controller.
public class BaselineSendingAgent {
    
    public class DataBroker {
        public String id_;
        public Socket sd_;
        public ObjectOutputStream os_;
        public ObjectInputStream is_;

        public DataBroker(String id, Socket sd) {
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
        public DataBroker data_Broker_;
        public LightFlow flow_;
        public String ra_ip_; // IP address of receiving agent
        public Socket sd_;
//        public OutputStream os_;
        private DataOutputStream dos; // FIXME(jimmy): not throttling this for now.
//        private BufferedOutputStream bos; // try this,  maybe higher performance.
//        private ThrottledOutputStream tos;

        public int buffer_size_ = 1024*1024;
        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
        public byte[] buffer_ = new byte[buffer_size_];

        public Sender(DataBroker dataBroker, String flow_id, double volume, String ra_ip) {
            data_Broker_ = dataBroker;
            flow_ = new LightFlow(flow_id, volume);
            ra_ip_ = ra_ip;

            try {
                sd_ = new Socket(ra_ip, 33330);
                dos = new DataOutputStream(new BufferedOutputStream(sd_.getOutputStream()));
//                tos = new ThrottledOutputStream(sd_.getOutputStream(), Constants.DEFAULT_OUTPUTSTREAM_RATE);
//                os_ = sd_.getOutputStream();
//                dos = new DataOutputStream(os_);
//                bos = new BufferedOutputStream(dos);
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void run() {
            while (flow_.transmitted_ < flow_.volume_) {
                try {
//                    os_.write(buffer_);
                    System.out.println("BaselineSA: Writing 1MB @ " + System.currentTimeMillis());
//                    tos.write(buffer_);
//                    os_.write(buffer_);
                    dos.write(buffer_);
//                    bos.write(buffer_);
                    System.out.println("BaselineSA: Finished Writing 1MB @ " + System.currentTimeMillis());
                }
                catch (java.io.IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                // We track how much we've sent in terms of megabits
                flow_.transmitted_ += (buffer_size_megabits_);
                System.out.println("BaselineSA: sent: " + flow_.transmitted_ + " for flow: " + flow_.id_);
            }

            try {
                data_Broker_.writeMessage(new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_.id_));
                System.out.println("BaselineSA: flow " + flow_.id_ + " completed. Not closing socket..");
//                sd_.close();
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Listens for messages from the controller and dispatches threads
    // to send flows.
    public class Listener implements Runnable {
        public DataBroker data_Broker_;
        
        public Listener(DataBroker d) {
            data_Broker_ = d;
        }

        public void run() {
            try {
                while (true) {
                    ControlMessage c = (ControlMessage) data_Broker_.is_.readObject();

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(data_Broker_.id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        (new Thread(new Sender(data_Broker_, c.flow_id_, c.field1_, "10.0.0." + (Integer.parseInt(c.ra_id_) + 1)))).start();
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
                        System.out.println(data_Broker_.id_ + " received an unexpected ControlMessage");
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

    public DataBroker data_Broker_;
    public Thread listener_;

    public BaselineSendingAgent(String id, Socket client_sd) {

        data_Broker_ = new DataBroker(id, client_sd);
        listener_ = new Thread(new Listener(data_Broker_));
        listener_.start();
    }


}
