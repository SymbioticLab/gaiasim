package gaiasim.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;

import gaiasim.network.NetGraph;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;
import gaiasim.util.Constants;

public class Manager {
    public NetGraph net_graph_;

    // Jobs indexed by id
    public HashMap<String, Job> jobs_;

    // Jobs sorted in increasing order of arrival time
    public Vector<Job> jobs_by_time_;

    public long CURRENT_TIME_;

    // Jobs that are currently being worked on 
    // TODO(jack): Consider making this a HashMap
    public ArrayList<Job> active_jobs_ = new ArrayList<Job>();
    public int num_active_jobs_ = 0;

    public Manager(String gml_file, String trace_file) throws java.io.IOException {
        net_graph_ = new NetGraph(gml_file);
        jobs_ = DAGReader.read_trace(trace_file, net_graph_);

        // Create sorted vector of jobs
        jobs_by_time_ = new Vector<Job>();
        for (String id : jobs_.keySet()) {
            jobs_by_time_.addElement(jobs_.get(id));
        }
        Collections.sort(jobs_by_time_, new Comparator<Job>() {
            public int compare(Job o1, Job o2) {
                if (o1.start_time_ == o2.start_time_) return 0;
                return o1.start_time_ < o2.start_time_ ? -1 : 1;
            }
        });
    }

    public void simulate() {
        int num_dispatched_jobs = 0;
        int total_num_jobs = jobs_.size();

        for (CURRENT_TIME_ = 0; 
                (num_dispatched_jobs < total_num_jobs) || (num_active_jobs_ > 0);
                    CURRENT_TIME_ += Constants.EPOCH_MILLI) {
            
            // Add any jobs which should be added during this epoch
            for (; num_dispatched_jobs < total_num_jobs; num_dispatched_jobs++) {
                Job j = jobs_by_time_.get(num_dispatched_jobs);

                // If the next job to start won't start during this epoch, no
                // further jobs should be considered.
                if (j.start_time_ >= (CURRENT_TIME_ + Constants.EPOCH_MILLI)) {

                    // TODO(jack): Add method which may be called here.
                    // Perhaps we want the emulator to simply sleep while waiting.
                    break;
                }
               
                active_jobs_.add(j);
                num_active_jobs_++;
                
            } // dispatch jobs loop

            System.out.printf("Timestep: %6d Running: %3d Started: %5d\n", 
                              CURRENT_TIME_, num_active_jobs_, num_dispatched_jobs);
        }
    }
}
