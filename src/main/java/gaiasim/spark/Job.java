package gaiasim.spark;

import gaiasim.network.Coflow;

import java.util.ArrayList;
import java.util.HashMap;

// A job within a trace
public class Job {

    public String id_;
    public long start_time_;
    public HashMap<String, Coflow> coflows_ = new HashMap<String, Coflow>();
    public ArrayList<Coflow> start_coflows_ = new ArrayList<Coflow>();
    public ArrayList<Coflow> end_coflows_ = new ArrayList<Coflow>();
    public boolean started_ = false;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;

    // Coflows that are currently running
    public ArrayList<Coflow> running_coflows_ = new ArrayList<Coflow>();

    // Coflows taht are ready to begin but have not begun yet
    public ArrayList<Coflow> ready_coflows_ = new ArrayList<Coflow>();

    public Job(String id, long start_time, HashMap<String, Coflow> coflows) {
        id_ = id;
        coflows_ = coflows;
        start_time_ = start_time;

        // Determine the end coflows of the DAG (those without any children).
        // Determine the start coflows of the DAG (those without parents).
        for (String key : coflows_.keySet()) {
            Coflow c = coflows_.get(key);
            if (c.child_coflows.size() == 0) {
                end_coflows_.add(c);
            }

            if (c.parent_coflows.size() == 0) {
                start_coflows_.add(c);
            } else {
                // Flows are created by depedent coflows. Since
                // starting coflows do not depend on anyone, they 
                // should not create coflows.
                c.create_flows();
            }
        }
    }

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

    // Remove s all of the parent Coflows depending on it. If any parent
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
    public ArrayList<Coflow> get_running_coflows() {
        running_coflows_.addAll(ready_coflows_);
        ready_coflows_.clear();

        // Return a clone of the list so that the calling function
        // can call finish_coflow while iterating over this list.
        return (ArrayList<Coflow>) running_coflows_.clone();
    }

    // Start all of the first coflows of the job, old version.
    // It ignores the start_coflows because by old definition these are void coflows.
    public void start() {
        for (Coflow c : start_coflows_) {
            c.done_ = true; // BEWARE! This is a hack!
            // Coflows are defined from parent stage to child stage,
            // so we add the start stage's children first.
            for (Coflow child : c.child_coflows) {
                if (!ready_coflows_.contains(child)) {
                    if (child.done()) {
                        // Hack to avoid error in finish_coflow
                        running_coflows_.add(child);
                        finish_coflow(child.id_);
                    }
                    // Add coflows which can be scheduled as a whole
                    else if (child.ready()) {
                        ready_coflows_.add(child);
                    }
                }
            } // for child_coflows

        } // for start_coflows_
        started_ = true;
    }

    // new Job.start(), used with DAGReader().
    public void start_New() {
        for (Coflow cf : start_coflows_) {
            if (!ready_coflows_.contains(cf)) {
                // we only proceed if
                if (cf.done()) { // This should never happen
                    System.err.println("ERROR: Root coflow " + cf.id_ + " is done when starting");
                    System.exit(1);
                }
                // Add coflows which can be scheduled as a whole
                else if (cf.ready()) {
                    ready_coflows_.add(cf);
                }
            }
        }
    }
}
