package gaiasim.spark;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.spark.Stage;

// A job within a trace
public class Job {

    public String id_; 
    public long start_time_;
    public HashMap<String, Stage> stages_ = new HashMap<String, Stage>();
    public ArrayList<Stage> start_stages_ = new ArrayList<Stage>();
    public ArrayList<Stage> end_stages_ = new ArrayList<Stage>();

    // Stages that are currently running
    public ArrayList<Stage> running_stages_ = new ArrayList<Stage>();

    // Stages taht are ready to begin but have not begun yet
    public ArrayList<Stage> ready_stages_ = new ArrayList<Stage>();

    public Job(String id, long start_time, HashMap<String, Stage> stages) {
        id_ = id;
        stages_ = stages;
        start_time_ = start_time;

        // Determine the end stages of the DAG (those without any parents).
        // Determine the start stages of the DAG (those without children).
        for (String key : stages_.keySet()) {
            Stage s = stages_.get(key);
            if (s.parent_stages_.size() == 0) {
                end_stages_.add(s);
            }

            if (s.child_stages_.size() == 0) {
                start_stages_.add(s);
            }
            else {
                // Coflows are created by depedent stages. Since
                // starting stages do not depend on anyone, they 
                // should not create coflows.
                s.create_coflow();
            }
        }
    }

    // A Job is considered done once all of its end stages have completed.
    public boolean done() {
        for (Stage s : end_stages_) {
            if (!s.done) {
                return false;
            }
        }

        return true;
    }

    // Remove s all of the parent Stages depending on it. If any parent
    // Stages are now ready to run, add them to ready_stages_.
    public void finish_coflow(String coflow_id) {
        Stage s = stages_.get(coflow_id);
        s.done = true;
        for (Stage parent : s.parent_stages_) {
            if (parent.ready()) {
                ready_stages_.add(parent);             
            }
        }

        running_stages_.remove(s);
    }

    // Transition all ready stages to running and return a list of all
    // coflows that are currently running for this job.
    public ArrayList<Coflow> get_running_coflows() {
        ArrayList<Coflow> running_coflows = new ArrayList<Coflow>();

        running_stages_.addAll(ready_stages_);
        ready_stages_.clear();
        
        for (Stage s : running_stages_) {
            running_coflows.add(s.coflow_); 
        }
        
        return running_coflows;
    }

    // Start all of the first stages of the job
    public void start() {
        for (Stage s : start_stages_) {
            // Coflows are defined from parent stage to start stage,
            // so we add the start stage first.
            ready_stages_.addAll(s.parent_stages_);
        }
    }
}
