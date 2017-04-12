package gaiasim.agent;

import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.agent.FlowInfo;

// Wrapper around a socket between a SA-RA pair.
public class Connection {

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

    public class ConnectionData {
        public String id_;

        // Queue on which SendingAgent places updates for this Connection. Updates
        // may inform the Connection of a new subscribing flow, an unsubscribing
        // flow, or that the Connection should terminate.
        public LinkedBlockingQueue<SubscriptionMessage> subscription_queue_ = 
            new LinkedBlockingQueue<SubscriptionMessage>();

        // The following two variables should probably be volatile,
        // but a small lack of visibility between two threads likely
        // won't cause enough damage to merit the perfomance loss
        // caused by making these frequently-accessed objects voltile.
        public HashMap<String, Subscription> subscribers_ = new HashMap<String, Subscription>();
        
        // Current total rate requested by subscribers
        public double rate_ = 0.0;

        public Socket sd_;
        public OutputStream os_;

        public ConnectionData(String id, Socket sd) {
            id_ = id;
            sd_ = sd;

            try {
                os_ = sd_.getOutputStream();
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

    private class Sender implements Runnable {
        public ConnectionData data_;
        public int buffer_size_ = 1024*1024;
        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
        public byte[] buffer_ = new byte[buffer_size_];

        public Sender(ConnectionData data) {
            data_ = data;
        }

        public void run() {
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
                            System.out.println("Subscribing flow " + m.flow_info_.id_ + " to " + data_.id_);
                            data_.rate_ += m.rate_;
                            data_.subscribers_.put(m.flow_info_.id_, new Subscription(m.flow_info_, m.rate_));
                        }
                    }
                    else if (m.type_ == MsgType.UNSUBSCRIBE) {
                        if (m.flow_info_.commit_unsubscription(data_.id_, m.ts_)) {
                            System.out.println("Unsubscribing flow " + m.flow_info_.id_ + " from " + data_.id_);
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
                            data_.sd_.close();
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
                    System.out.println(data_.id_ + " transmitting with " + data_.subscribers_.size() + " and rate " + data_.rate_);
                    try {
                        data_.os_.write(buffer_);
                        data_.distribute_transmitted(buffer_size_megabits_); 
                    }
                    catch (java.io.IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            } // while (true)
        } // run()
    } // class Sender

    public ConnectionData data_;
    public Thread sending_thread_;

    public Connection(String id, Socket sd) {
        data_ = new ConnectionData(id, sd);
        
        sending_thread_ = new Thread(new Sender(data_));
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
