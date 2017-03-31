package gaiasim.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

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

    public class ConnectionData {
        public String id_;

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
            rate_ += rate;
            subscribers_.put(f.id_, new Subscription(f, rate));
        }

        public synchronized void unsubscribe(String id) {
            Subscription s = subscribers_.get(id);
            rate_ -= s.rate_;
            subscribers_.remove(id);
        }

    }

    private class Sender implements Runnable {
        public ConnectionData data_;

        public Sender(ConnectionData data) {
            data_ = data;
        }

        public void run() {
            while (!Thread.interrupted()) {
                Random rnd = new Random(13);

                if (data_.rate_ > 0.0) {
                    // TODO: Actually send data
                    data_.distribute_transmitted(1048576 * (rnd.nextDouble() + 0.5));
                }
            }
            // TODO: Close socket
            return;
        }
    } // class Sender

    public ConnectionData data_;
    public Thread sending_thread_;

    public Connection(String id) {
        data_ = new ConnectionData(id);

        // TODO: Start Sender thread
        sending_thread_ = new Thread(new Sender(data_));
        sending_thread_.start();
    } 

    public void terminate() {
        sending_thread_.interrupt();
    }

}
