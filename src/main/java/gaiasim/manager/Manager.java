package gaiasim.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.BaselineScheduler;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;
import gaiasim.util.Constants;

public class Manager {
    public NetGraph net_graph_;

    public Scheduler scheduler_;

    // Jobs indexed by id
    public HashMap<String, Job> jobs_;

    // Jobs sorted in increasing order of arrival time
    public Vector<Job> jobs_by_time_;

    public long CURRENT_TIME_;

    // Jobs and Coflows that are currently being worked on 
    public HashMap<String, Job> active_jobs_ = new HashMap<String, Job>();
    public HashMap<String, Coflow> active_coflows_ = new HashMap<String, Coflow>();
    public HashMap<String, Flow> active_flows_ = new HashMap<String, Flow>();

    public Manager(String gml_file, String trace_file, String scheduler_type) throws java.io.IOException {
        net_graph_ = new NetGraph(gml_file);
        jobs_ = DAGReader.read_trace(trace_file, net_graph_);

        if (scheduler_type.equals("baseline")) {
            scheduler_ = new BaselineScheduler(net_graph_);
        }
        else if (scheduler_type.equals("recursive-remain-flow")) {
            System.out.println("recursive-remain-flow not currenlty implemented");
            System.exit(1);
        }
        else {
            System.out.println("Unrecognized scheduler type: " + scheduler_type);
            System.out.println("Scheduler must be one of { baseline, recursive-remain-flow }");
            System.exit(1);
        }

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
        
        ArrayList<Job> ready_jobs = new ArrayList<Job>();

        for (CURRENT_TIME_ = 0; 
                (num_dispatched_jobs < total_num_jobs) || !active_jobs_.isEmpty();
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
              
                ready_jobs.add(j);
                
            } // dispatch jobs loop

            if (!ready_jobs.isEmpty()) {
                // TODO(jack): Trigger an interrupt
                
                for (Job j : ready_jobs) {
                    // Start arriving jobs
                    if (!j.started_) {
                        j.start_timestamp_ = CURRENT_TIME_;
                        j.start();
                    }

                    active_jobs_.put(j.id_, j);
                }
               
                // Update our set of active coflows
                active_coflows_.clear();
                for (String k : active_jobs_.keySet()) {
                    Job j = active_jobs_.get(k);

                    ArrayList<Coflow> coflows = j.get_running_coflows();
                    for (Coflow c : coflows) {
                        System.out.println("Adding coflow " + c.id_);
                        active_coflows_.put(c.id_, c);

                        if (c.start_timestamp_ == -1) {
                            c.start_timestamp_ = CURRENT_TIME_;
                        }
                    }
                }

                // TODO(jack): Trigger a reschedule event

                // Update our set of flows
                active_flows_.clear();
                active_flows_.putAll(scheduler_.schedule_flows(active_coflows_, CURRENT_TIME_));
                ready_jobs.clear();
            }
            
            // List to keep track of flow keys that have finished
            ArrayList<Flow> finished = new ArrayList<Flow>();

            // Make progress on all running flows
            for (long ts = Constants.SIMULATION_TIMESTEP_MILLI; 
                    ts <= Constants.EPOCH_MILLI; 
                    ts += Constants.SIMULATION_TIMESTEP_MILLI) {
                
                for (String k : active_flows_.keySet()) {
                    Flow f = active_flows_.get(k);

                    f.transmitted_ += f.rate_ * Constants.SIMULATION_TIMESTEP_MILLI;
                    if (f.transmitted_ >= f.volume_) {
                        finished.add(f);
                    }
                }

                // Handle flows which have completed
                for (Flow f : finished) {
                    active_flows_.remove(f.id_);
                    f.done = true;
                    f.end_timestamp_ = CURRENT_TIME_ + ts;
                    System.out.println("Flow " + f.id_ + " done. Took "+ (f.end_timestamp_ - f.start_timestamp_));

                    // After completing a flow, an owning coflow may have been completed
                    Coflow owning_coflow = active_coflows_.get(f.coflow_id_);
                    if (owning_coflow.done()) {
                        owning_coflow.end_timestamp_ = CURRENT_TIME_ + ts;
                        System.out.println("Coflow " + f.coflow_id_ + " done. Took " 
                                                + (owning_coflow.end_timestamp_ - owning_coflow.start_timestamp_));
                        owning_coflow.done_ = true;
                        
                        // After completing a coflow, an owning job may have been completed
                        Job owning_job = active_jobs_.get(Constants.get_job_id(owning_coflow.id_));
                        owning_job.finish_coflow(owning_coflow.id_);

                        if (owning_job.done()) {
                            owning_job.end_timestamp_ = CURRENT_TIME_ + ts;
                            System.out.println("Job " + owning_job.id_ + " done. Took "
                                                + (owning_job.end_timestamp_ - owning_job.start_timestamp_)); 
                            active_jobs_.remove(owning_job.id_);
                        }
                        else {
                            ready_jobs.add(owning_job);     
                        }

                    } // if coflow.done
                } // for finished

                finished.clear();

            } // for EPOCH_MILLI

            System.out.printf("Timestep: %6d Running: %3d Started: %5d\n", 
                              CURRENT_TIME_, active_jobs_.size(), num_dispatched_jobs);

        }
    }
}
