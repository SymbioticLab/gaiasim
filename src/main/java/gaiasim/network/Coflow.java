package gaiasim.network;

// ver 1.1 fixed naming of parent_coflows and child_coflows

import java.util.ArrayList;
import java.util.HashMap;

// A coflow represents a shuffle within a job. It is an edge within a DAG.
public class Coflow {

    public String id_;
    public HashMap<String, Flow> flows_ = new HashMap<>();
    public double volume_ = 0.0;
    public long last_scheduled_timestamp = -1;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;
    public boolean done_ = false;
    public boolean isTrimmed = false;
    public boolean dropped = false;

    // The location of coflow-initiating tasks. For example, these would be
    // the locations of map tasks in a map-reduce shuffle.
    public String[] task_locs_;

    // Coflows that this coflow depends on (must complete before this
    // coflow starts).
    public ArrayList<Coflow> parent_coflows = new ArrayList<>();

    // Coflows which depend on this Coflow (this Coflow must complete
    // before child Coflows start).
    public ArrayList<Coflow> child_coflows = new ArrayList<>();

    // DarkScheduler-specific variables
    public int current_queue_ = 0;
    public double transmitted_ = 0.0;
    public Integer ddl_Millis = -1;
    public double minCCT = -2;

    public Coflow(String id, String[] task_locs) {
        id_ = id;
        task_locs_ = task_locs;
    }

    // Sets the coflow's start time to be that of the earliest starting flow.
    // Assumes all flows are done.
    public void determine_start_time() {
        start_timestamp_ = Long.MAX_VALUE;
        for (String k : flows_.keySet()) {
            Flow f = flows_.get(k);
            if (f.start_timestamp_ < start_timestamp_) {
                start_timestamp_ = f.start_timestamp_;
            }
        }
    }

    // Return whether owned Flows are done
    public boolean done() {
        if (!done_) {
            for (String k : flows_.keySet()) {
                if (!flows_.get(k).done_) {
                    return false;
                }
            }
            done_ = true;
        }
        return true;
    }

    // Returns whether the Coflow can begin or not. A Coflow can begin
    // only if all of the Coflows on which it depends have completed.
    public boolean ready() {
        for (Coflow s : parent_coflows) {
            if (!s.done_) {
                return false;
            }
        }

        return true;
    }
}

