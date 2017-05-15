package gaiasim.agent;

import com.revinate.guava.util.concurrent.RateLimiter;
import gaiasim.util.Constants;

import java.io.*;
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
        public double total_rate = 0.0; // TODO(jimmy): track the total_rate.

        public Socket dataSocket;

//        public OutputStream dataOutputStream;  // deprecated
//        private ThrottledOutputStream tos; // Use a standalone rate limiter.


        private BufferedOutputStream bos;
//        private final RateLimiter rateLimiter;
//        private DataOutputStream dos;
//        private BufferedWriter bw;

        public ConnectionDataBroker(String id, Socket sd) {
            id_ = id;
            dataSocket = sd;


            try {
                bos = new BufferedOutputStream(dataSocket.getOutputStream() , Constants.BUFFER_SIZE );

//                tos = new ThrottledOutputStream( dataSocket.getOutputStream() , Constants.DEFAULT_OUTPUTSTREAM_RATE); // init rate to be 100kByte/s
//                bos = new BufferedOutputStream(tos);
//                dos = new DataOutputStream(tos);
//                bw = new BufferedWriter( new OutputStreamWriter(tos));
//                dataOutputStream = dataSocket.getOutputStream();
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

        }


        public synchronized void distribute_transmitted(double transmitted_MBit) {
            if (transmitted_MBit > 0.0) {

                ArrayList<Subscription> to_remove = new ArrayList<Subscription>();
                FlowInfo f;
                double flow_rate;
                for (String k : subscribers_.keySet()) {
                    Subscription s = subscribers_.get(k);
                    f = s.flow_info_;
                    flow_rate = s.rate_;

                    boolean done = f.transmit(transmitted_MBit * flow_rate / total_rate, id_);
                    if (done) {
                        to_remove.add(s);
                    }
                }

                for (Subscription s : to_remove) {
                    total_rate -= s.rate_;
                    subscribers_.remove(s.flow_info_.id_);
                }

                // Ensure we don't get rounding errors
                if (subscribers_.isEmpty()) {
                    total_rate = 0.0;
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
        private final RateLimiter rateLimiter;
        public ConnectionDataBroker data_;
//        public int buffer_size_ = 1024*1024;
//        public int buffer_size_ = Constants.BUFFER_SIZE; // currently 1MB buffer
//        public int buffer_size_megabits_ = buffer_size_ / 1024 / 1024 * 8;
//        public byte[] buffer_ = new byte[buffer_size_];

        private byte[] data_block = new byte[Constants.BLOCK_SIZE_MB * 1024 * 1024]; // 32MB for now.

        public SenderThread(ConnectionDataBroker data) {
            rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);
            data_ = data;
        }

        public void run() {
            // nested while loop
            while (true) {
                SubscriptionMessage m = null;

                // If we don't currently have any subscribers (rate = 0),
                // then data_block until we get some subscription message (take()).
                // Otherwise, check if there's a subscription message, but
                // don't data_block if there isn't one (poll()).
                if (data_.total_rate <= 0.0) {
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
                            data_.total_rate += m.rate_;
                            data_.subscribers_.put(m.flow_info_.id_, new Subscription(m.flow_info_, m.rate_));
                        }
                    }
                    else if (m.type_ == MsgType.UNSUBSCRIBE) {
                        if (m.flow_info_.commit_unsubscription(data_.id_, m.ts_)) {
                            System.out.println("PersistentConn: Unsubscribing flow " + m.flow_info_.id_ + " from " + data_.id_);
                            Subscription s = data_.subscribers_.get(m.flow_info_.id_);
                            data_.total_rate -= s.rate_;
                            data_.subscribers_.remove(m.flow_info_.id_);
                            
                            // Ensure there aren't any rounding errors
                            if (data_.subscribers_.isEmpty()) {
                                data_.total_rate = 0.0;
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
                if (data_.total_rate > 0.0) {
                    try {
                        // rate is MBit/s, converting to Block/s

                        double cur_rate = data_.total_rate / Constants.CHEATING_FACTOR; // FIXME: also cheat here.

                        int data_length;

                        // check if 100 permits/s is enough (3200MByte/s enough?)
                        if( cur_rate < Constants.BLOCK_SIZE_MB * 8 * Constants.DEFAULT_TOKEN_RATE  ){
                            // no need to change rate , calculate the length
                            rateLimiter.setRate(Constants.DEFAULT_TOKEN_RATE);
                            data_length = (int) (cur_rate / Constants.DEFAULT_TOKEN_RATE * 1024 * 1024 / 8);
                        }
                        else {
                            data_length = Constants.BLOCK_SIZE_MB;
                            rateLimiter.setRate(cur_rate / 8 / Constants.BLOCK_SIZE_MB);
                        }

                        // aquire one permit per flush.
                        rateLimiter.acquire(1);

                        data_.bos.write(data_block , 0, data_length);
                        data_.bos.flush();

//                        System.out.println("PersistentConn: Flushed Writing " + data_length + " w/ rate: " + data_.total_rate + " Mbit/s  @ " + System.currentTimeMillis());
                        System.out.println("PersistentConn: Flushed Writing w/ rate: " + data_.total_rate + " Mbit/s @ " + System.currentTimeMillis());

                        // distribute transmitted...
                        double tx_ed = (double) data_length * 8 / 1024 / 1024;

                        data_.distribute_transmitted(Constants.CHEATING_FACTOR * tx_ed); // FIXME: cheat here!
//                        System.out.println("T_MBit " + tx_ed + " original " + buffer_size_megabits_);
//                        data_.distribute_transmitted(buffer_size_megabits_);
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
