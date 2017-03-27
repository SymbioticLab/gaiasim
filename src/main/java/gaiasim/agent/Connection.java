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

    public HashMap<String, Subscription> subscribers_ = new HashMap<String, Subscription>();

    // Current total rate requested by subscribers
    public double rate_ = 0.0;

    public String id_;

    public Connection(String id, String ra_ip, String ra_port) {
        id_ = id;

        // TODO: Setup a socket with designated receiving agent
        
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

                boolean flow_done = f.transmit(transmitted * flow_rate / rate_, this);
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

    public void send() {
        Random rnd = new Random(13);

        if (rate_ > 0.0) {
            // TODO: Actually send data
            distribute_transmitted(1048576 * (rnd.nextDouble() + 0.5));
        }
    }
    
    public synchronized void subscribe(FlowInfo f, double rate) {
        rate_ += rate;
        subscribers_.put(f.id_, new Subscription(f, rate));
    }
}
