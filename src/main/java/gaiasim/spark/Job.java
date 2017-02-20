package gaiasim.spark;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.network.Coflow;

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

        // Determine the end coflows of the DAG (those without any parents).
        // Determine the start coflows of the DAG (those without children).
        for (String key : coflows_.keySet()) {
            Coflow c = coflows_.get(key);
            if (c.parent_coflows_.size() == 0) {
                end_coflows_.add(c);
            }

            if (c.child_coflows_.size() == 0) {
                start_coflows_.add(c);
            }
            else {
                // Flows are created by depedent coflows. Since
                // starting coflows do not depend on anyone, they 
                // should not create coflows.
                c.create_flows();
            }
        }
    }

    // A job is considered done if all of its coflows are done
    public boolean done() {
        for (String k : coflows_.keySet()) {
            Coflow c = coflows_.get(k);
            if (!c.done()) {
                return false;
            }
        }

        return true;
    }

    // Remove s all of the parent Coflows depending on it. If any parent
    // Coflows are now ready to run, add them to ready_coflows_.
    public void finish_coflow(String full_coflow_id) {
        // A coflow's id is of the form <job_id>:<coflow_id> whereas
        // our coflow map is indxed by <coflow_id>. Retrieve the coflow_id here.
        String coflow_id = full_coflow_id.split(":")[1];
        Coflow c = coflows_.get(coflow_id);
        running_coflows_.remove(c);

        for (Coflow parent : c.parent_coflows_) {
            if (parent.ready()) {
                // If all of the parent's tasks are located in the same node
                // as our tasks, then there aren't any flows needed for the
                // parent to complete. Register the parent as done and
                // recursively add any coflows dependent on it.
                if (parent.done()) {
                    running_coflows_.add(parent);
                    parent.start_timestamp_ = end_timestamp_;
                    parent.end_timestamp_ = end_timestamp_;
                    System.out.println("Coflow " + parent.id_ + " done. Took 0");

                    finish_coflow(parent.id_);
                }
                else {
                    ready_coflows_.add(parent);
                }

            } // parent.ready()

        } // for parent_coflows_

    }

    // Transition all ready coflows to running and return a list of all
    // coflows that are currently running for this job.
    public ArrayList<Coflow> get_running_coflows() {
        running_coflows_.addAll(ready_coflows_);
        ready_coflows_.clear();
        
        return running_coflows_;
    }

    // Start all of the first coflows of the job
    public void start() {
        for (Coflow c : start_coflows_) {
            c.done_ = true; 
            // Coflows are defined from parent stage to child stage,
            // so we add the start stage's parents first.
            for (Coflow parent : c.parent_coflows_) {
                if (!ready_coflows_.contains(parent)) {
                    if (parent.done()) {
                        // Hack to avoid error in finish_coflow
                        running_coflows_.add(parent);
                        finish_coflow(parent.id_);
                    }
                    // Add coflows which can be scheduled as a whole
                    else if (parent.ready()) {
                        ready_coflows_.add(parent);
                    }
                }
            } // for parent_coflows_

        } // for start_coflows_
        started_ = true;
    }
}
