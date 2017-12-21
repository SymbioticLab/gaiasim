package gaiasim.manager;

import com.opencsv.CSVWriter;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.*;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;
import gaiasim.util.Constants;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

@SuppressWarnings("Duplicates")

public class Manager {
    public NetGraph net_graph_;

    public Scheduler scheduler_;

    // Path to directory to save output files
    public String outdir_;

    // Jobs indexed by id
    public HashMap<String, Job> jobs_;

    // Jobs sorted in increasing order of arrival time
    public Vector<Job> jobs_by_time_ = new Vector<>();
    public Vector<Job> completed_jobs_ = new Vector<>();
    public ArrayList<Coflow> completed_coflows_ = new ArrayList<>();

    public long CURRENT_TIME_;

    // Jobs and Coflows that are currently being worked on 
    public HashMap<String, Job> active_jobs_ = new HashMap<>();
    public HashMap<String, Coflow> active_coflows_ = new HashMap<>();
    public HashMap<String, Flow> active_flows_ = new HashMap<>();

    // Some state variables moved out of simulate():
    boolean coflow_finished = false;    // Whether a coflow finished
    boolean isOneByOne;

    public int droppedCnt = 0;

    public Manager(String gml_file, String trace_file,
                   String scheduler_type, String outdir,
                   double bw_factor, double workload_factor, boolean is_one_by_one) throws java.io.IOException {
        outdir_ = outdir;
        net_graph_ = new NetGraph(gml_file, bw_factor);
        jobs_ = DAGReader.read_trace_new(trace_file, net_graph_, workload_factor);

        this.isOneByOne = is_one_by_one;

        if (scheduler_type.equals("baseline")) {
            scheduler_ = new BaselineScheduler(net_graph_);
        } else if (scheduler_type.equals("recursive-remain-flow")) {
            scheduler_ = new PoorManScheduler(net_graph_);
        } else if (scheduler_type.equals("multipath")) {
            scheduler_ = new MultiPathScheduler(net_graph_);
        } else if (scheduler_type.equals("varys")) {
            scheduler_ = new VarysScheduler(net_graph_);
        } else if (scheduler_type.equals("swan")) {
            scheduler_ = new SwanScheduler(net_graph_);
        } else if (scheduler_type.equals("dark")) {
            scheduler_ = new DarkScheduler(net_graph_);
        } else if (scheduler_type.equals("rapier")) {
            scheduler_ = new RapierScheduler(net_graph_);
        } else {
            System.out.println("Unrecognized scheduler type: " + scheduler_type);
            System.out.println("Scheduler must be one of { baseline, recursive-remain-flow }");
            System.exit(1);
        }

        // Create sorted vector of jobs
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

    public void handle_finished_coflow(Coflow c, long cur_time) throws java.io.IOException {
        c.determine_start_time();
        c.end_timestamp_ = cur_time;
        System.out.println("Coflow " + c.id_ + " done. Took "
                + (c.end_timestamp_ - c.start_timestamp_));
        c.done_ = true;

        completed_coflows_.add(c);

        // After completing a coflow, an owning job may have been completed
        Job owning_job = active_jobs_.get(Constants.get_job_id(c.id_));

        // An owning job may also been aborted upon this coflow finish
        if (owning_job == null){
            return;
        }

        owning_job.finish_coflow(c.id_);

        if (owning_job.done()) {
            owning_job.end_timestamp_ = cur_time;
            System.out.println("Job " + owning_job.id_ + " done. Took "
                    + (owning_job.end_timestamp_ - owning_job.start_timestamp_));
            active_jobs_.remove(owning_job.id_);
            completed_jobs_.addElement(owning_job);
            print_statistics("/tmp_job.csv", "/tmp_cct.csv");
        }
    }

    public void print_statistics(String job_filename, String coflow_filename) throws java.io.IOException {
        String job_output = outdir_ + job_filename;

        // Create directory if it doesn't exist
        File file = new File(job_output);
        file.getParentFile().mkdirs();

        CSVWriter writer = new CSVWriter(new FileWriter(file), ',');
        String[] record = new String[4];
        record[0] = "JobID";
        record[1] = "StartTime";
        record[2] = "EndTime";
        record[3] = "JobCompletionTime";
        writer.writeNext(record);
        for (Job j : completed_jobs_) {
            record[0] = j.id_;
            record[1] = Double.toString(j.start_timestamp_ / Constants.MILLI_IN_SECOND_D);
            record[2] = Double.toString(j.end_timestamp_ / Constants.MILLI_IN_SECOND_D);
            record[3] = Double.toString((j.end_timestamp_ - j.start_timestamp_) / Constants.MILLI_IN_SECOND_D);
            writer.writeNext(record);
        }
        writer.close();

        String coflow_output = outdir_ + coflow_filename;
        CSVWriter c_writer = new CSVWriter(new FileWriter(coflow_output), ',');
        record[0] = "CoflowID";
        record[1] = "StartTime";
        record[2] = "EndTime";
        record[3] = "CoflowCompletionTime";
        c_writer.writeNext(record);

        Collections.sort(completed_coflows_, new Comparator<Coflow>() {
            public int compare(Coflow o1, Coflow o2) {
                if (o1.start_timestamp_ == o2.start_timestamp_) return 0;
                return o1.start_timestamp_ < o2.start_timestamp_ ? -1 : 1;
            }
        });

        for (Coflow c : completed_coflows_) {
            record[0] = c.id_;
            record[1] = Double.toString(c.start_timestamp_ / Constants.MILLI_IN_SECOND_D);
            record[2] = Double.toString(c.end_timestamp_ / Constants.MILLI_IN_SECOND_D);
            record[3] = Double.toString((c.end_timestamp_ - c.start_timestamp_) / Constants.MILLI_IN_SECOND_D);
            c_writer.writeNext(record);
        }
        c_writer.close();
    }

    public void simulate() throws Exception {
        int num_dispatched_jobs = 0;
        int total_num_jobs = jobs_.size();
        int last_num_jobs = -1;
        int last_job_size = -1;
        long last_time = -5000;

        int stallingCounter = 0;

        ArrayList<Job> ready_jobs = new ArrayList<>();

        for (CURRENT_TIME_ = 0;
             (num_dispatched_jobs < total_num_jobs) || !active_jobs_.isEmpty();
             CURRENT_TIME_ += Constants.EPOCH_MILLI) {

            if (isOneByOne){
                if(active_jobs_.isEmpty()){
                    Job j = jobs_by_time_.get(num_dispatched_jobs);
                    ready_jobs.add(j);
                    num_dispatched_jobs++;
                }
            }
            else {
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
            }

            for (Map.Entry<String, Coflow> cfe : active_coflows_.entrySet()){
                Coflow cf = cfe.getValue();
                if (cf.dropped){
                    cf.done_ = true;

                    scheduler_.remove_coflow(cf);

                    Job owning_job = active_jobs_.get(Constants.get_job_id(cf.id_));

                    // also drop owning_job
                    if (owning_job != null){
                        active_jobs_.remove(Constants.get_job_id(cf.id_));
                    }

                }
            }

            // essentially YARN logic: (i) insert job (ii) handle CF_FIN
            if (coflow_finished || !ready_jobs.isEmpty()) {

                coflow_finished = false; // clean the flag first

                for (Job j : ready_jobs) {
                    // Start arriving jobs
                    // NOTE: This assumes that JCT is measured as the time as (job_finish_time - job_arrival_time)
                    if (!j.started_) {
                        j.start_timestamp_ = CURRENT_TIME_;
                        j.start_New();
                    }

                    // The next coflow in the job may be the last coflow in the job. If the stages involved
                    // in that coflow are collocated, then there's nothing for us to do. This could cause
                    // the job to be marked as done.
                    if (j.done()) {
                        j.end_timestamp_ = CURRENT_TIME_;
                        System.out.println("Job " + j.id_ + " done. Took " + (j.end_timestamp_ - j.start_timestamp_));
                    } else {
                        active_jobs_.put(j.id_, j);
                    }
                }

                // Update our set of active coflows: insert CF whenever ready
                active_coflows_.clear();
                // use iterator so that we can delete jobs that finishes with co-located CF.
                Iterator<Map.Entry<String, Job>> iter = active_jobs_.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Job> e = iter.next();
                    Job j = e.getValue();

                    ArrayList<Coflow> coflows = j.get_running_coflows(CURRENT_TIME_);
                    for (Coflow c : coflows) {
                        if (c.done()) { //  would never happen
                            c.start_timestamp_ = CURRENT_TIME_;
                            handle_finished_coflow(c, CURRENT_TIME_);
                            System.err.println("ERROR: CF done before init");
                        } else {
                            // check CF before adding to ensure no co-located FlowGroups
                            System.out.println("Checking coflow " + c.id_);

                            // if the CF is not fully trimmed, insert it. If fully trimmed, no output for it.
                            if (!trimCoflow(c, CURRENT_TIME_)) {
                                active_coflows_.put(c.id_, c);
                                scheduler_.add_coflow(c);
                            } else {
                                System.out.println("CF trimmed");

                                // mark the CF as done in YARN, we still need to handle the finish of this coflow
                                // handle_finished_coflow(c, CURRENT_TIME_); // ConcurrentModificationException

                                coflow_finished = true;

                                c.start_timestamp_ = CURRENT_TIME_;
                                c.end_timestamp_ = CURRENT_TIME_;
                                c.done_ = true;
                                j.finish_coflow(c.id_);

                                // We still need to print out these kind of jobs..
                                completed_coflows_.add(c);
                                if (j.done()) {
                                    j.end_timestamp_ = CURRENT_TIME_;
                                    iter.remove();
                                    completed_jobs_.addElement(j);
                                    print_statistics("/tmp_job.csv", "/tmp_cct.csv");
                                }

                            }
                        }
                    }
                }

                // Update our set of flows
                active_flows_.clear();
                HashMap<String, Flow> scheduled_flows = scheduler_.schedule_flows(active_coflows_, CURRENT_TIME_);
                active_flows_.putAll(scheduled_flows);
                ready_jobs.clear(); // all the jobs in ready_jobs have been inserted, hence clearing it.
            } // End of YARN code

            // List to keep track of flow keys that have finished
            ArrayList<Flow> finished = new ArrayList<>();

            // Keep track of total allocated bandwidth across ALL flows
            double totalBW = 0.0;

            // Make progress on all running flows
            for (long ts = Constants.SIMULATION_TIMESTEP_MILLI;
                 ts <= Constants.EPOCH_MILLI;
                 ts += Constants.SIMULATION_TIMESTEP_MILLI) {

                // Reset for each EPOCH
                finished.clear();
                totalBW = 0.0;

                for (String k : active_flows_.keySet()) {
                    Flow f = active_flows_.get(k);

                    totalBW += scheduler_.progress_flow(f);
                    if (f.transmitted_ + Constants.EPSILON >= f.volume_) { // ignoring the remaining 0.01MBit
                        finished.add(f);
                        f.transmitted_ = f.volume_; // so that the remain_volume = 0
                    }
                }

                // Handle flows which have completed
                for (Flow f : finished) {
                    active_flows_.remove(f.id_);
                    f.done_ = true;
                    f.end_timestamp_ = CURRENT_TIME_ + ts;
                    scheduler_.finish_flow(f);
                    System.out.println("Flow " + f.id_ + " done. Took " + (f.end_timestamp_ - f.start_timestamp_));

                    // After completing a flow, an owning coflow may have been completed
                    Coflow owning_coflow = active_coflows_.get(f.coflow_id_);
                    if (owning_coflow.done()) {
                        handle_finished_coflow(owning_coflow, CURRENT_TIME_ + ts);
                        coflow_finished = true;
                        scheduler_.remove_coflow(owning_coflow);
                    } // if coflow.done
                } // for finished

                // If any flows finished during this round, update the bandwidth allocated
                // to each active flow.
                if (!finished.isEmpty()) {
                    scheduler_.update_flows(active_flows_);
                }
            } // for EPOCH_MILLI

            // Not printing Timestamp every time. every 1s or every change happens.
            if (num_dispatched_jobs != last_num_jobs || active_jobs_.size() != last_job_size || (CURRENT_TIME_ - last_time >= 1000)) {
                System.out.printf("Timestep: %6d Running: %3d Started: %5d BW: %10.0f\n",
                        CURRENT_TIME_ + Constants.EPOCH_MILLI, active_jobs_.size(), num_dispatched_jobs, totalBW);

                // Adds stalling detector
                if (num_dispatched_jobs >= jobs_.size() && active_flows_.isEmpty() && !active_jobs_.isEmpty()) {
                    stallingCounter++;
                    if (stallingCounter > 100) {
                        System.err.println("Simulation stalled");
                        System.exit(1);
                    }
                }

                last_job_size = active_jobs_.size();
                last_num_jobs = num_dispatched_jobs;
                last_time = CURRENT_TIME_;
            }
        } // while stuff to do

        System.out.println("Total dropped: " + scheduler_.droppedCnt + " . missed after admission (contains duplicate): " + scheduler_.missDDLCNT);
        System.out.println("Simulation DONE");

        // Save output statistics
        print_statistics("/job.csv", "/cct.csv");
    }

    public boolean trimCoflow(Coflow cf, long curTime) {
        boolean cfFinished = true; // init a flag

        // need iterator because we need to remove while iterating
        Iterator<Map.Entry<String, Flow>> iter = cf.flows_.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Flow> entry = iter.next();
            Flow f = entry.getValue();
            if (f.dst_loc_ == f.src_loc_) { // job is co-located.

                // Finish this FG right away. And remove the entry
                f.end_timestamp_ = curTime + Constants.COLLOCATED_FG_COMPLETION_TIME;
                f.done_ = true;
                iter.remove();

                System.out.println("Flow " + f.id_ + " ignored (due to co-location) ");
            } else {
                cfFinished = false; // more than one FlowGroup are not trimmed (need to be transmitted).
            }
        }

        return cfFinished;
    }
}
