package gaiasim.agent;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import gaiasim.comm.ControlMessage;
import gaiasim.comm.ScheduleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Acts on behalf of the controller to start transfers
// from one node to another. Does not keep any persistent
// connections between nodes -- every flow gets its own
// socket and traverses the topology selected by the normal
// routing strategy. Once started, flows are not preempted
// by the controller.
public class BaselineSendingAgent {
    private final boolean isCloudLab;

//    private static final Logger logger = LoggerFactory.getLogger("SendingAgent.class");
//    ExecutorService es = new ThreadPoolExecutor(10, 100, 10, );
//    ExecutorService es = Executors.newFixedThreadPool(100);

    
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
//        private DataOutputStream dos; // some people say you should never use this
        private BufferedOutputStream bos; // FIXME(jimmy): not throttling this for now.
//        private ThrottledOutputStream tos;

        public int buffer_size_ = 1024*1024;
        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
        public byte[] buffer_ = new byte[buffer_size_];
        private String threadName;

        public Sender(DataBroker dataBroker, String flow_id, double volume, String ra_ip) {
            data_Broker_ = dataBroker;
            flow_ = new LightFlow(flow_id, volume);
            ra_ip_ = ra_ip;
            threadName = Thread.currentThread().getName();

        }

        public void run() {


            System.out.println(Thread.currentThread().getName() + " starts working on " + flow_.id_);
            while (flow_.transmitted_ < flow_.volume_) {

                // first try to establish the connection
                try {
                    sd_ = new Socket(ra_ip_, 33330);
                    bos = new BufferedOutputStream(sd_.getOutputStream());
                }
                catch (java.io.IOException e) {
                    e.printStackTrace();
                    System.out.println( threadName + " Exception while connecting, retry");
                    continue;
//                    System.exit(1);
                }

                // try to send the data
                while (flow_.transmitted_ < flow_.volume_) {
                    try {
//                    System.out.println("BaselineSA: Writing 1MB @ " + System.currentTimeMillis());
                        bos.write(buffer_);
                        bos.flush(); // it is important to flush
//                    System.out.println("BaselineSA: Flushed Writing 1MB @ " + System.currentTimeMillis());
                    } catch (java.io.IOException e) {
                        e.printStackTrace();
                        System.out.println( threadName + " Exception while sending, re-connecting");
                        break;
                    }

                    // We track how much we've sent in terms of megabits
                    flow_.transmitted_ += (buffer_size_megabits_);
                    System.out.println( threadName + "BaselineSA: sent: " + flow_.transmitted_ + " for flow: " + flow_.id_);
                }
            } // end of sending all data

            try {
                data_Broker_.writeMessage(new ScheduleMessage(ScheduleMessage.Type.FLOW_COMPLETION, flow_.id_));
                System.out.println("BaselineSA: flow " + flow_.id_ + " completed. Now closing socket to RA");

                bos.close(); // clean up
                sd_.close(); // we have to close the socket here!!!!
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.out.println(threadName + "Exception when closing socket");
            }

            System.out.println(Thread.currentThread().getName() + " says goodbye ");
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
                while (true) { // when to jump out of the loop?
                    ControlMessage c = (ControlMessage) data_Broker_.is_.readObject();

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(data_Broker_.id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");

                        String raIP = null;
                        if (isCloudLab) {
                            // raIP for cloudlab
                            raIP = "10.0." + (Integer.parseInt(c.ra_id_) + 1) + "." + (Integer.parseInt(c.ra_id_) + 1);
                        }
                        else {
                            raIP = "10.0.0." + (Integer.parseInt(c.ra_id_) + 1);
                        }

                        (new Thread(new Sender(data_Broker_, c.flow_id_, c.field1_, raIP))).start();
/*                        try {
                            es.submit(new Sender(data_Broker_, c.flow_id_, c.field1_, "10.0.0." + (Integer.parseInt(c.ra_id_) + 1)));
                        }
                        catch (RejectedExecutionException e){
                            e.printStackTrace();
                        }*/
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
                        System.out.println("BSA: Received TERMINATE.");
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

    public DataBroker data_Broker;
    public Thread listener_;

    public BaselineSendingAgent(String id, Socket client_sd, boolean isCloudlab) throws InterruptedException {

        data_Broker = new DataBroker(id, client_sd);
        this.isCloudLab = isCloudlab;
//        listener_ = new Thread(new Listener(data_Broker));
//        listener_.start();
//        listener_.join(); // waiting for it to die.

        Listener listener = new Listener(data_Broker);
        listener.run(); // equivalent to above.
    }


}
