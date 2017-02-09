package gaiasim.scheduler;

import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.Scheduler;

public class BaselineScheduler extends Scheduler {
    // Persistent map used ot hold temporary data. We simply clear it
    // when we need it to hld new data rather than creating another
    // new map object (avoid GC).
    private HashMap<String, Flow> flows_ = new HashMap<String, Flow>();

    public BaselineScheduler(NetGraph net_graph) {
        super(net_graph);
    }

    public HashMap<String, Flow> schedule_flows(HashMap<String, Coflow> coflows, 
                                                long timestamp) {
        flows_.clear();
        for (String k : coflows.keySet()) {
            Coflow c = coflows.get(k);

            for (String k_ : c.flows_.keySet()) {
                Flow f = c.flows_.get(k_);

                // TODO(jack): Actually ad scheduling part to get rates for flows
                f.rate_ = 10.0;

                if (f.start_timestamp_ == -1) {
                    f.start_timestamp_ = timestamp;
                }

                flows_.put(f.id_, f);
            }
        }

        return flows_;
    }
}
