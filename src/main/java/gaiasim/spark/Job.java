package gaiasim.spark;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.spark.Stage;

// A job within a trace
public class Job {

    public String id_; 
    public long start_time_;
    public HashMap<String, Stage> stages_ = new HashMap<String, Stage>();
    public ArrayList<Stage> start_stages_ = new ArrayList<Stage>();
    public ArrayList<Stage> end_stages_ = new ArrayList<Stage>();

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
    // Completed end stages will be removed from end_stages_.
    public boolean done() {
        return end_stages_.size() == 0;
    }
}
