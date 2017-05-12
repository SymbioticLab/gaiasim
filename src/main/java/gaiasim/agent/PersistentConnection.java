package gaiasim.agent;

import gaiasim.util.Constants;
import gaiasim.util.ThrottledOutputStream;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

// This class is NOT used in baseline.
// Wrapper around a socket between a SA-RA pair.
// DataBroker is persistently sent through this socket.

public class PersistentConnection {

    public class Subscription {
        public FlowInfo flow_info_;
        public double rate_ = 0.0;

        public Subscription(FlowInfo f, double rate) {
            flow_info_ = f;
            rate_ = rate;
        }
    }

    public enum MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        TERMINATE
    }

    public class SubscriptionMessage { 
        public MsgType type_;
        public FlowInfo flow_info_;
        public double rate_ = 0.0; // Only used for SUBSCRIBE
        public long ts_ = 0; // Only used for (UN)SUBSCRIBE

        // TERMINATE
        public SubscriptionMessage(MsgType type) {
            type_ = type;
        }

        // UNSUBSCRIBE
        public SubscriptionMessage(MsgType type, FlowInfo f, long ts) {
            type_ = type;
            flow_info_ = f;
            ts_ = ts;
        }

        // SUBSCRIBE
        public SubscriptionMessage(MsgType type, FlowInfo f, double rate, long ts) {
            type_ = type;
            flow_info_ = f;
            rate_ = rate;
            ts_ = ts;
        }
    }

    public class ConnectionDataBroker {
        public String id_;

        // Queue on which SendingAgent places updates for this PersistentConnection. Updates
        // may inform the PersistentConnection of a new subscribing flow, an unsubscribing
        // flow, or that the PersistentConnection should terminate.
        public LinkedBlockingQueue<SubscriptionMessage> subscription_queue_ = 
            new LinkedBlockingQueue<SubscriptionMessage>();

        // TODO: The following two variables should probably be volatile,
        // but a small lack of visibility between two threads likely
        // won't cause enough damage to merit the perfomance loss
        // caused by making these frequently-accessed objects voltile.
        public HashMap<String, Subscription> subscribers_ = new HashMap<String, Subscription>();
        
        // Current total rate requested by subscribers
        public double rate_ = 0.0; // TODO(jimmy): track the rate_.

        public Socket dataSocket;

//        public OutputStream dataOutputStream;  // deprecated
        private ThrottledOutputStream tos; // TODO: (wrap a buffered writer around this!)
        private DataOutputStream dos;
//        private BufferedWriter bw;

        public ConnectionDataBroker(String id, Socket sd) {
            id_ = id;
            dataSocket = sd;

            try {
                tos = new ThrottledOutputStream( dataSocket.getOutputStream() , Constants.DEFAULT_OUTPUTSTREAM_RATE); // init rate to be 100
                dos = new DataOutputStream(tos);
//                bw = new BufferedWriter( new OutputStreamWriter(tos));
//                dataOutputStream = dataSocket.getOutputStream();
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

        }


        public synchronized void distribute_transmitted(double transmitted) {
            if (transmitted > 0.0) {

                ArrayList<Subscription> to_remove = new ArrayList<Subscription>();
                FlowInfo f;
                double flow_rate;
                for (String k : subscribers_.keySet()) {
                    Subscription s = subscribers_.get(k);
                    f = s.flow_info_;
                    flow_rate = s.rate_;

                    boolean done = f.transmit(transmitted * flow_rate / rate_, id_);
                    if (done) {
                        to_remove.add(s);
                    }
                }

                for (Subscription s : to_remove) {
                    rate_ -= s.rate_;
                    subscribers_.remove(s.flow_info_.id_);
                }

                // Ensure we don't get rounding errors
                if (subscribers_.isEmpty()) {
                    rate_ = 0.0;
                }
            }
        }

        public synchronized void subscribe(FlowInfo f, double rate, long update_ts) {
            SubscriptionMessage m = new SubscriptionMessage(MsgType.SUBSCRIBE, 
                                                            f, rate, update_ts);
            try {
                subscription_queue_.put(m);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public synchronized void unsubscribe(FlowInfo f, long update_ts) {
            SubscriptionMessage m = new SubscriptionMessage(MsgType.UNSUBSCRIBE, f, update_ts);
            
            try {
                subscription_queue_.put(m);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

    }

    private class SenderThread implements Runnable {
        public ConnectionDataBroker data_;
        public int buffer_size_ = 1024*1024;
        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
        public byte[] buffer_ = new byte[buffer_size_];

        public SenderThread(ConnectionDataBroker data) {
            data_ = data;
        }

        public void run() {
            // nested while loop
            while (true) {
                SubscriptionMessage m = null;

                // If we don't currently have any subscribers (rate = 0),
                // then block until we get some subscription message (take()).
                // Otherwise, check if there's a subscription message, but
                // don't block if there isn't one (poll()).
                if (data_.rate_ <= 0.0) {
                    try {
                        m = data_.subscription_queue_.take();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
                else {
                    m = data_.subscription_queue_.poll();
                }

                // m will be null only if poll() returned that we have no
                // messages. If m is not null, process the message.
                while (m != null) {
                    if (m.type_ == MsgType.SUBSCRIBE) {
                        if (m.flow_info_.commit_subscription(data_.id_, m.ts_)) {
                            System.out.println("PersistentConn: Subscribing flow " + m.flow_info_.id_ + " to " + data_.id_);
                            data_.rate_ += m.rate_;
                            data_.subscribers_.put(m.flow_info_.id_, new Subscription(m.flow_info_, m.rate_));
                        }
                    }
                    else if (m.type_ == MsgType.UNSUBSCRIBE) {
                        if (m.flow_info_.commit_unsubscription(data_.id_, m.ts_)) {
                            System.out.println("PersistentConn: Unsubscribing flow " + m.flow_info_.id_ + " from " + data_.id_);
                            Subscription s = data_.subscribers_.get(m.flow_info_.id_);
                            data_.rate_ -= s.rate_;
                            data_.subscribers_.remove(m.flow_info_.id_);
                            
                            // Ensure there aren't any rounding errors
                            if (data_.subscribers_.isEmpty()) {
                                data_.rate_ = 0.0;
                            }
                        }
                    }
                    else {
                        // TERMINATE
                        try {
                            data_.dataSocket.close();
                        }
                        catch (java.io.IOException e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                        return;
                    }

                    m = data_.subscription_queue_.poll();
                }

                // If we have some subscribers (rate > 0), then transmit on
                // behalf of the subscribers.
                if (data_.rate_ > 0.0) {
                    try {
                        // try to fill the outgoing buffer as fast as possible
                        // until all the outgoing data are "sent".

                        // TODO: get the thorttling right. the unit conversion
                        data_.tos.setRate(data_.rate_ * 1000 * 125);

                        // TODO(jimmy): use a throttled writer and block here.
//                        data_Broker.dataOutputStream.write(buffer_);
//                        data_Broker.dos.write(buffer_);
                        System.out.println("PersistentConn: Writing 1MB @ rate: " + data_.rate_ + " @ " + System.currentTimeMillis());
                        data_.tos.write(buffer_);
                        System.out.println("PersistentConn: Finished Writing 1MB @ rate: " + data_.rate_ + " @ " + System.currentTimeMillis());
                        // This is not efficient according to the implementation of OutputStream.write()
                        // This is supposed to be blocking, a.k.a., safe (no buffer overflow)
                        data_.distribute_transmitted(buffer_size_megabits_); 
                    }
                    catch (java.io.IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            } // while (true)
        } // run()
    } // class SenderThread

    public ConnectionDataBroker data_;
    public Thread sending_thread_;

    public PersistentConnection(String id, Socket sd) {
        data_ = new ConnectionDataBroker(id, sd);
        
        sending_thread_ = new Thread(new SenderThread(data_));
        sending_thread_.start();
    } 

    public void terminate() {
        try {
            data_.subscription_queue_.put(new SubscriptionMessage(MsgType.TERMINATE));
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
