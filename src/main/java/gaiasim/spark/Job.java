package gaiasim.spark;

import gaiasim.network.Coflow;

import java.util.ArrayList;
import java.util.HashMap;

// A job within a trace
public class Job {

    public String id_;
    public long start_time_;
    public HashMap<String, Coflow> coflows_ = new HashMap<>();
    public ArrayList<Coflow> start_coflows_ = new ArrayList<>();
    public ArrayList<Coflow> end_coflows_ = new ArrayList<>();
    public boolean started_ = false;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;

    // Coflows that are currently running
    public ArrayList<Coflow> running_coflows_ = new ArrayList<>();

    // Coflows that are ready to begin but have not begun yet
    public ArrayList<Coflow> ready_coflows_ = new ArrayList<>();

    // the new constructor. We don't call Coflow.create_flows() here.
    public Job(String id, long arrivalTime) {
        this.id_ = id;
        this.start_time_ = arrivalTime;
    }

    // A job is considered done if all of its coflows are done
    public boolean done() {
        for (String k : coflows_.keySet()) {
            Coflow c = coflows_.get(k);
            if (!(c.done() || c.flows_.isEmpty())) {
                return false;
            }
        }

        return true;
    }

    // Removes all of the parent Coflows depending on it. If any parent
    // Coflows are now ready to run, add them to ready_coflows_.
    public void finish_coflow(String full_coflow_id) {
        // No need to split to get partial coflow_id here.
        Coflow c = coflows_.get(full_coflow_id);
        running_coflows_.remove(c);

        for (Coflow child : c.child_coflows) {
            if (child.ready()) {
                ready_coflows_.add(child);
            }
        } // for child_coflows

    }

    // Transition all ready coflows to running and return a list of all
    // coflows that are currently running for this job.
    public ArrayList<Coflow> get_running_coflows(long current_timestamp) {
        for (Coflow c : ready_coflows_) {
            c.start_timestamp_ = current_timestamp;
        }
        running_coflows_.addAll(ready_coflows_);
        ready_coflows_.clear();

        // Return a clone of the list so that the calling function
        // can call finish_coflow while iterating over this list.
        return (ArrayList<Coflow>) running_coflows_.clone();
    }

    // new Job.start(), used with DAGReader().
    public void start_New() {
        for (Coflow cf : start_coflows_) {
            if (!ready_coflows_.contains(cf)) {
                // we only proceed if not co-located
                if (cf.done()) { //
                    System.out.println("WARNING: Root coflow " + cf.id_ + " is done when starting");
                }
                // Add coflows which can be scheduled as a whole
                else if (cf.ready()) {
                    ready_coflows_.add(cf);
                }
            }
        }
    }
}
