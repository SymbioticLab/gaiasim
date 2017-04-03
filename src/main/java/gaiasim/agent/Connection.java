package gaiasim.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
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
        public Subscription subscription_;

        public SubscriptionMessage(MsgType type) {
            type_ = type;
        }

        public SubscriptionMessage(MsgType type, Subscription s) {
            type_ = type;
            subscription_ = s;
        }

        public SubscriptionMessage(MsgType type, FlowInfo f, double rate) {
            type_ = type;
            subscription_ = new Subscription(f, rate);
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

        public ConnectionData(String id) {
            id_ = id;
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

                    boolean flow_done = f.transmit(transmitted * flow_rate / rate_, id_);
                    if (flow_done) {
                        to_remove.add(s);
                    }
                }

                // Unsubscribe any flows that were done
                for (Subscription s : to_remove) {
                    rate_ -= s.rate_;
                    subscribers_.remove(s.flow_info_.id_);
                }

                // Ensure we don't have any rounding errors
                if (subscribers_.isEmpty()) {
                    rate_ = 0.0;
                }
            }
        }

        public synchronized void subscribe(FlowInfo f, double rate) {
            SubscriptionMessage m = new SubscriptionMessage(MsgType.SUBSCRIBE, 
                                                            f, rate);
            try {
                subscription_queue_.put(m);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public synchronized void unsubscribe(String id) {
            Subscription s = subscribers_.get(id);
            SubscriptionMessage m = new SubscriptionMessage(MsgType.UNSUBSCRIBE, s);
            
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
                if (m != null) {
                    if (m.type_ == MsgType.SUBSCRIBE) {
                        data_.rate_ += m.subscription_.rate_;
                        data_.subscribers_.put(m.subscription_.flow_info_.id_, m.subscription_);
                    }
                    else if (m.type_ == MsgType.UNSUBSCRIBE) {
                        data_.rate_ -= m.subscription_.rate_;
                        data_.subscribers_.remove(m.subscription_.flow_info_.id_);
                    }
                    else {
                        // TERMINATE

                        // TODO: Close socket
                        return;
                    }
                }

                // If we have some subscribers (rate > 0), then transmit on
                // behalf of the subscribers.
                if (data_.rate_ > 0.0) {
                    data_.distribute_transmitted(data_.rate_);
                }
            } // while (true)
        } // run()
    } // class Sender

    public ConnectionData data_;
    public Thread sending_thread_;

    public Connection(String id) {
        data_ = new ConnectionData(id);
        
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
