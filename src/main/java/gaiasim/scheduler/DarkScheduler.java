package gaiasim.scheduler;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

public class DarkScheduler extends PoorManScheduler {
    private static int NUM_JOB_QUEUES = 10;
    private static double INIT_QUEUE_LIMIT = 10.0;
    private static double JOB_SIZE_MULT = 10.0;

    private Vector<Coflow>[] sortedCoflows;

    public DarkScheduler(NetGraph net_graph) {
        super(net_graph);

        sortedCoflows = (Vector<Coflow>[]) new Vector[NUM_JOB_QUEUES];
        for (int i = 0; i < NUM_JOB_QUEUES; i++) {
            sortedCoflows[i] = new Vector<>();
        }
    }

    @Override
    public double progress_flow(Flow f) {
        double totalBW = 0.0;
        for (Pathway p : f.paths_) {
            double t = p.bandwidth_ * Constants.SIMULATION_TIMESTEP_SEC;
            f.transmitted_ += t;
            f.owning_coflow_.transmitted_ += t;
            totalBW += p.bandwidth_ * ( p.node_list_.size() - 1 ); // actual BW usage = pathBW * hops
        }
        return totalBW;
    }

    @Override
    public void add_coflow(Coflow c) {
        for (int i = 0; i < NUM_JOB_QUEUES; i++) {
            if (sortedCoflows[i].contains(c)) {
                return;
            }
        }

        // Add to the end of the first queue
        sortedCoflows[0].add(c);
        c.current_queue_ = 0;
    }

    @Override
    public void remove_coflow(Coflow c) {
        for (int i = 0; i < NUM_JOB_QUEUES; i++) {
            if (sortedCoflows[i].remove(c)) {
                break;
            }
        }
    }

    @Override
    protected ArrayList<Coflow> sort_coflows(HashMap<String, Coflow> coflows) throws Exception {
        // Move them around
        for (int i = 0; i < NUM_JOB_QUEUES; i++) {
            Vector<Coflow> coflowsToMove = new Vector<>();
            for (Coflow c : sortedCoflows[i]) {
                double size = c.transmitted_;
                int curQ = 0;
                for (double k = INIT_QUEUE_LIMIT; k < size; k *= JOB_SIZE_MULT) {
                    curQ += 1;
                }
                if (c.current_queue_ < curQ) {
                    c.current_queue_ += 1;
                    coflowsToMove.add(c);
                }
            }
            if (i + 1 < NUM_JOB_QUEUES && coflowsToMove.size() > 0) {
                sortedCoflows[i].removeAll(coflowsToMove);
                sortedCoflows[i + 1].addAll(coflowsToMove);
            }
        }

        // Flatten and send back
        ArrayList<Coflow> cct_list = new ArrayList<>();
        for (int i = 0; i < NUM_JOB_QUEUES; i++) {
            cct_list.addAll(sortedCoflows[i]);
        }

        return cct_list;
    }
}
