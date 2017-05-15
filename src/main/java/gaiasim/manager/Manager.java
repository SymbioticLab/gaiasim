package gaiasim.manager;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.SendingAgentContact;
import gaiasim.comm.PortAnnouncementMessage;
import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.BaselineScheduler;
import gaiasim.scheduler.PoorManScheduler;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;
import gaiasim.spark.JobInserter;
import gaiasim.util.Constants;

public class Manager {
    public NetGraph net_graph_;

    public Scheduler scheduler_;

    // The number of jobs that have been inserted
    public int num_dispatched_jobs_ = 0;

    // Path to directory to save output files
    public String outdir_;

    // Jobs indexed by id
    public HashMap<String, Job> jobs_;

    // Jobs sorted in increasing order of arrival time
    public Vector<Job> jobs_by_time_ = new Vector<Job>();
    public Vector<Job> completed_jobs_ = new Vector<Job>();
    public ArrayList<Coflow> completed_coflows_ = new ArrayList<Coflow>();

    public long CURRENT_TIME_;

    // Jobs and Coflows that are currently being worked on 
    public HashMap<String, Job> active_jobs_ = new HashMap<String, Job>();
    public HashMap<String, Coflow> active_coflows_ = new HashMap<String, Coflow>();
    public HashMap<String, Flow> active_flows_ = new HashMap<String, Flow>();

    public LinkedBlockingQueue<ScheduleMessage> message_queue_ =
        new LinkedBlockingQueue<ScheduleMessage>();

    // SendingAgentContacts indexed by sending agent id
    public HashMap<String, SendingAgentContact> sa_contacts_ = 
        new HashMap<String, SendingAgentContact>();

    public boolean is_baseline_ = false;
    private long lastScheduledAt;

    public Manager(String gml_file, String trace_file, 
                   String scheduler_type, String outdir) throws java.io.IOException {
        outdir_ = outdir;
        net_graph_ = new NetGraph(gml_file);
        jobs_ = DAGReader.read_trace(trace_file, net_graph_);

        lastScheduledAt = System.currentTimeMillis();

        if (scheduler_type.equals("baseline")) {
            scheduler_ = new BaselineScheduler(net_graph_);
            is_baseline_ = true;
        }
        else if (scheduler_type.equals("recursive-remain-flow")) {
            scheduler_ = new PoorManScheduler(net_graph_);
        }
        else {
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
        c.determine_start_time();  // FIXME: it must be that sometime start_time = Long.max
        c.end_timestamp_ = cur_time;
        System.out.println("Coflow " + c.id_ + " done. Took " + (c.end_timestamp_ - c.start_timestamp_));
        c.done_ = true;

        completed_coflows_.add(c);
        
        // After completing a coflow, an owning job may have been completed
        Job owning_job = active_jobs_.get(Constants.get_job_id(c.id_));
        owning_job.finish_coflow(c.id_);

        if (owning_job.done()) {
            owning_job.end_timestamp_ = cur_time;
            System.out.println("Manager/handle_FIN_coflow: Job " + owning_job.id_ + " done. Took "
                    + (owning_job.end_timestamp_ - owning_job.start_timestamp_)); 
            active_jobs_.remove(owning_job.id_);
            completed_jobs_.addElement(owning_job);
            print_statistics("/tmp_job.csv", "/tmp_cct.csv");
        }
    }

    // Returns true if the completion of this flow caused a coflow
    // to be completed.
    public boolean handle_finished_flow(Flow f, long cur_time) throws java.io.IOException {
        active_flows_.remove(f.id_);
        f.transmitted_ = f.volume_;
        f.done_ = true;
        f.end_timestamp_ = cur_time;
        scheduler_.finish_flow(f);
        System.out.println("Flow " + f.id_ + " done. Took "+ (f.end_timestamp_ - f.start_timestamp_));

        // After completing a flow, an owning coflow may have been completed
        Coflow owning_coflow = active_coflows_.get(f.coflow_id_);
        if (owning_coflow.done()) {
            handle_finished_coflow(owning_coflow, cur_time); 
            return true;
        } // if coflow.done

        return false;
    }

    public void print_statistics(String job_filename, String coflow_filename) throws java.io.IOException {
        String job_output = outdir_ + job_filename;
        CSVWriter writer = new CSVWriter(new FileWriter(job_output), ',');
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

    public void emulate() throws Exception {
        LinkedBlockingQueue<PortAnnouncementMessage> port_announcements = 
            new LinkedBlockingQueue<PortAnnouncementMessage>();

        // Set up our SendingAgentContacts
        for (String sa_id : net_graph_.nodes_) {
            sa_contacts_.put(sa_id, 
                             new SendingAgentContact(sa_id, net_graph_, "10.0.0." + (Integer.parseInt(sa_id) + 1), 23330, 
                                                     message_queue_, port_announcements, is_baseline_));
        }
      
        // If we aren't emulating baseline, receive the port announcements
        // from SendingAgents and set appropriate flow rules.
        if (!is_baseline_) {
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(net_graph_, port_announcements);
            relay.relay_ports();
        }

        num_dispatched_jobs_ = 0;
        int total_num_jobs = jobs_.size();

        long emulation_start_time = System.currentTimeMillis();

        // Start inserting jobs
        Thread job_inserter = new Thread(new JobInserter(jobs_by_time_, message_queue_));
        job_inserter.start();

        // TODO: new event loop. now we only test coflow.
        if(!is_baseline_) {

            System.out.println("Coflow scheduling, enter new event loop!");


            while ((num_dispatched_jobs_ < total_num_jobs) || !active_jobs_.isEmpty()) {
                // Block until we have a message to receive
                ScheduleMessage m = message_queue_.take(); // maybe use poll with timeout 1second? no! we don't know how long!

                switch (m.type_) {
                    case JOB_INSERTION:
                        System.out.println("Manager: Job coming: " + m.jobs);

                        onJobInsertion(m);

                        break;

                    case FLOW_COMPLETION:
                        System.out.println("Manager: Received FLOW_COMPLETION " + m.flow_id_);

                        onFlowCompletion(m);


                        break;


                    case FLOW_STATUS_RESPONSE:
//                        System.out.println("Manager: Received STATUS " + m.flow_id_);

                        onStatusResponse(m);


                        break;


                }
            }
        }
        else { // Old event loop. (for baseline only)
            // Handle job insertions and flow updates
            try {
                while ((num_dispatched_jobs_ < total_num_jobs) || !active_jobs_.isEmpty()) {
                    // Block until we have a message to receive
                    ScheduleMessage m = message_queue_.take(); // JobInserter inserts messages by time. (emulating online job coming)

                    if (m.type_ == ScheduleMessage.Type.JOB_INSERTION) {
                        System.out.println("Manager: Job coming: " + m.jobs);
                        addBatchJobs(m);
//                        System.out.println("Manager: Job " + m.job_id_ + " comes, now rescheduling");
//                        Job j = jobs_.get(m.job_id_);
//                        start_job(j);

                        if (is_baseline_) {
                            for(String job_id : m.jobs){
                                // for each job
                                Job j = jobs_.get(job_id);

                                // TODO: check safety here.
                                add_next_flows_for_job(j, System.currentTimeMillis());
                            }

                        } else {
                            reschedule();
                        }
                    } else if (m.type_ == ScheduleMessage.Type.FLOW_COMPLETION) {
                        System.out.println("Received FLOW_COMPLETION for Flow " + m.flow_id_);

                        Flow f = active_flows_.get(m.flow_id_);
                        long current_time = System.currentTimeMillis();
                        boolean coflow_finished = handle_finished_flow(f, System.currentTimeMillis());
                        if (coflow_finished) {
                            System.out.println(" Some Coflow finished, results in reschedule");
                            if (is_baseline_) {

                                // There is a chance that completing this coflow finished the
                                // job. If this is the case, the call to handle_finished_flow will
                                // have removed the job from active_jobs_ and thus j will be null.
                                // Therefore, if j is null, we know that the job must be done and
                                // that there are no more flows to add. There's probably a better
                                // way to check for this.
                                Job j = active_jobs_.get(Constants.get_job_id(f.id_));
                                if (j != null) {
                                    add_next_flows_for_job(j, current_time);
                                }
                            } else {
                                reschedule();
                            }
                        }
                    } else if (m.type_ == ScheduleMessage.Type.FLOW_STATUS_RESPONSE) {
                        System.err.println("ERROR: Received a flow status response for " + m.flow_id_ + " while controller was not expecting flow status responses");
                        System.exit(1);
                    }
                }
            } catch (InterruptedException e) {
                // This shouldn't happen. Fail if it does.
                e.printStackTrace();
                System.exit(1);
            }
        }

        System.out.println("DONE. Elapsed time: " + (System.currentTimeMillis() - emulation_start_time) + " ms");

        // Terminate all SendingAgents
        for (String k : sa_contacts_.keySet()) {
            SendingAgentContact sac = sa_contacts_.get(k);
            sac.terminate();
        }

        print_statistics("/job.csv", "/cct.csv");
    }

    // non-blocking handler of status response, only for coflow
    private void onStatusResponse(ScheduleMessage m) {


        if(canSchedule()){
            schedule_Jimmy();
        }
    }

    // non-blocking handler of flow completion, only for coflow
    private void onFlowCompletion(ScheduleMessage m) {


        if(canSchedule()){
            schedule_Jimmy();
        }
    }

    private void addBatchJobs(ScheduleMessage m){
        for(String job_id : m.jobs){
            // for each job
            Job j = jobs_.get(job_id);

            // add to active job list.
            start_job(j);
        }
    }

    // non-blocking handler of job insertion, (only for coflow)
    private void onJobInsertion(ScheduleMessage m) {
        // first add job then check the batch pool, if timestamp is the same, batch
        // if not the same flush previous jobs, batch this one.
        // no need to batch here.

        // first add all the jobs to active_jobs
        addBatchJobs(m);


        // check state, if
        // then check if we need schedule(reschedule)

        if(canSchedule()){
            schedule_Jimmy();
        }

        // this is previous implementation
//        if (is_baseline_) {
//            add_next_flows_for_job(j, System.currentTimeMillis());
//        }
//        else {
//            reschedule();
//        }

    }

    private boolean canSchedule() {
        // first check the timing, last schedule happens 1s away?
        long cur_time = System.currentTimeMillis();
        if(cur_time - lastScheduledAt < 800){ // padding for some overhead, so we don't miss job coming in the next second.
            return false;
        }


        // then check do we have things to schedule?
        if(active_jobs_.isEmpty() || active_coflows_.isEmpty() || active_flows_.isEmpty() ){
            return false;
        }


        // then check do we have all the info we need? STATUS etc.




        return false;
    }

    // we schedule because: 1. job insertion 2. flow status change (including completion)
    // since last schedule is at least 1s away, we query status for every schedule.
    private void schedule_Jimmy() {
        // first set the schedule time
        lastScheduledAt = System.currentTimeMillis();
        System.out.println("Manger: reschedule @ " + lastScheduledAt);

        // then query the status? no!




    }

    private void requestAllFlowStatus(){
        for (String sa_id : sa_contacts_.keySet()) {
            SendingAgentContact sac = sa_contacts_.get(sa_id);
            sac.send_status_request();
        }
    }

    // Used by baseline scheduler to start the next flows of a job
    public void add_next_flows_for_job(Job j, long current_time) throws Exception {
        ArrayList<Coflow> coflows = j.get_running_coflows();
        HashMap<String, Coflow> coflow_map = new HashMap<String, Coflow>();
        for (Coflow c : coflows) {
            if (c.done()) {
                c.start_timestamp_ = current_time;
                handle_finished_coflow(c, current_time);
            }
            else {
                System.out.println("Adding coflow " + c.id_);
                active_coflows_.put(c.id_, c);
                coflow_map.put(c.id_, c);
            }
        }
 
        HashMap<String, Flow> scheduled_flows = scheduler_.schedule_flows(coflow_map, current_time);
        active_flows_.putAll(scheduled_flows);
        for (String flow_id : scheduled_flows.keySet()) {
            Flow f = scheduled_flows.get(flow_id);

            if (!f.started_sending_) {
                sa_contacts_.get(f.src_loc_).start_flow(f);

                // Only update started_sending_ if we're running baseline
                f.started_sending_ = is_baseline_;
            }
        }

    }

    // Used by coflow scheduler to preempt running flows and reschedule
    // running flows.
    public void reschedule() throws Exception {
        System.out.println("Manager: entered reschedule()");

        ArrayList<Flow> preempted_flows = new ArrayList<Flow>();

        // Send FLOW_STATUS_REQUEST to all SA_Contacts
        if (!active_flows_.isEmpty()) {
            for (String sa_id : sa_contacts_.keySet()) {
                SendingAgentContact sac = sa_contacts_.get(sa_id);
                sac.send_status_request();
            }
      
            // Wait until we've received either a FLOW_COMPLETION or
            // FLOW_STATUS_RESPONSE for every active flow
            while (!active_flows_.isEmpty()) {
                try {
                    // Block until we have a message to receive
                    ScheduleMessage m = message_queue_.take();

                    if (m.type_ == ScheduleMessage.Type.JOB_INSERTION) {
                        System.out.println("Manager: Job coming: " + m.jobs);
                        addBatchJobs(m);
//                        Job j = jobs_.get(m.job_id_);
//                        start_job(j);
                    }
                    else if (m.type_ == ScheduleMessage.Type.FLOW_COMPLETION) {
                        System.out.println("Registering FLOW_COMPLETION for " + m.flow_id_);
                        Flow f = active_flows_.get(m.flow_id_);
                        handle_finished_flow(f, System.currentTimeMillis());
                    }
                    else if (m.type_ == ScheduleMessage.Type.FLOW_STATUS_RESPONSE) {
                        Flow f = active_flows_.get(m.flow_id_);
                        System.out.println("Registering FLOW_STATUS_RESPONSE for " + m.flow_id_ + " transmitted " + m.transmitted_ + " of " + f.volume_);
                        f.transmitted_ = m.transmitted_;
                        f.updated_ = true;
                        preempted_flows.add(f);
                        active_flows_.remove(m.flow_id_);
                    }
                }
                catch (InterruptedException e) {
                    // This shouldn't happen. Fail if it does.
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        // Reschedule the current flows
        update_and_schedule_flows(System.currentTimeMillis());

        for (Flow f : preempted_flows) {
            if (!active_flows_.containsKey(f.id_)) {
                active_flows_.put(f.id_, f);
            }
        }

        // Send FLOW_UPDATEs and FLOW_STARTs based on new schedule
        // TODO: Consider parallelizing this so that messages intended
        //       for different SAs don't block on each other.
        for (String flow_id : active_flows_.keySet()) {
            Flow f = active_flows_.get(flow_id);

            if (!f.started_sending_) {
                sa_contacts_.get(f.src_loc_).start_flow(f);

                // Only update started_sending_ if we're running baseline
                f.started_sending_ = is_baseline_;
            }

            // we only start new flow here, and (modify/preempt) existing flows..?
            // If a flow is going to be preempted, it has to be in active_flows. a bit paradox! FIXME
        }

    }

    public void simulate() throws Exception {
        int num_dispatched_jobs_ = 0;
        int total_num_jobs = jobs_.size();

        ArrayList<Job> ready_jobs = new ArrayList<Job>();

        // Whether a coflow finished in the last epoch
        boolean coflow_finished = false;

        for (CURRENT_TIME_ = 0; 
                (num_dispatched_jobs_ < total_num_jobs) || !active_jobs_.isEmpty();
                    CURRENT_TIME_ += Constants.EPOCH_MILLI) {

            // Add any jobs which should be added during this epoch
            for (; num_dispatched_jobs_ < total_num_jobs; num_dispatched_jobs_++) {
                Job j = jobs_by_time_.get(num_dispatched_jobs_);

                // If the next job to start won't start during this epoch, no
                // further jobs should be considered.
                if (j.start_time_ >= (CURRENT_TIME_ + Constants.EPOCH_MILLI)) {

                    // TODO(jack): Add method which may be called here.
                    // Perhaps we want the emulator to simply sleep while waiting.
                    break;
                }
              
                ready_jobs.add(j);
                
            } // dispatch jobs loop

            if (coflow_finished || !ready_jobs.isEmpty()) {
                
                for (Job j : ready_jobs) {
                    // Start arriving jobs
                    // NOTE: This assumes that JCT is measured as the time as (job_finish_time - job_arrival_time)
                    if (!j.started_) {
                        j.start_timestamp_ = CURRENT_TIME_;
                        j.start();
                    }

                    // The next coflow in the job may be the last coflow in the job. If the stages involved
                    // in that coflow are colocated, then there's nothing for us to do. This could cause
                    // the job to be marked as done.
                    if (j.done()) {
                        j.end_timestamp_ = CURRENT_TIME_;
                        System.out.println("Manger/simulate: Job " + j.id_ + " done. Took " + (j.end_timestamp_ - j.start_timestamp_));
                        completed_jobs_.addElement(j);
                    }
                    else {
                        active_jobs_.put(j.id_, j);
                    }
                }

                update_and_schedule_flows(CURRENT_TIME_);
                ready_jobs.clear();
            }

            coflow_finished = false;
            
            // List to keep track of flow keys that have finished
            ArrayList<Flow> finished = new ArrayList<Flow>();

            // Make progress on all running flows
            for (long ts = Constants.SIMULATION_TIMESTEP_MILLI; 
                    ts <= Constants.EPOCH_MILLI; 
                    ts += Constants.SIMULATION_TIMESTEP_MILLI) {
                
                for (String k : active_flows_.keySet()) {
                    Flow f = active_flows_.get(k);

                    scheduler_.progress_flow(f);

                    // If there's less than one bit remaining of the flow, consider it fully
                    // transmitted.
                    if ((f.volume_ - f.transmitted_) < 1/*f.transmitted_ >= f.volume_*/) {
                        finished.add(f);
                    }
                }

                // Handle flows which have completed
                for (Flow f : finished) {
                    boolean caused_coflow_finish = handle_finished_flow(f, CURRENT_TIME_ + ts);
                    coflow_finished = coflow_finished || caused_coflow_finish;
                } // for finished

                // If any flows finished during this round, update the bandwidth allocated
                // to each active flow.
                if (!finished.isEmpty()) {
                    scheduler_.update_flows(active_flows_);
                }

                finished.clear();

            } // for EPOCH_MILLI

            System.out.printf("Timestep: %6d Running: %3d Started: %5d\n", 
                              CURRENT_TIME_ + Constants.EPOCH_MILLI, active_jobs_.size(), num_dispatched_jobs_);

        } // while stuff to do

        // Save output statistics
        print_statistics("/job.csv", "/cct.csv");
    }

    public void start_job(Job j) {
        // Start arriving job
        // NOTE: This assumes that JCT is measured as the time as (job_finish_time - job_arrival_time)
        j.start_timestamp_ = System.currentTimeMillis();
        j.start();

        // The next coflow in the job may be the last coflow in the job. If the stages involved
        // in that coflow are colocated, then there's nothing for us to do. This could cause
        // the job to be marked as done.
        if (j.done()) { // Testing if the job is done instantly.
            j.end_timestamp_ = j.start_timestamp_;
            System.out.println("Manager/start_job: Job " + j.id_ + " done. (done instantly) Took " + (j.end_timestamp_ - j.start_timestamp_));
            completed_jobs_.addElement(j);
        }
        else {
            active_jobs_.put(j.id_, j);
        }

        num_dispatched_jobs_++;
    }

    public void update_and_schedule_flows(long current_time) throws Exception {
        // Update our set of active coflows
        active_coflows_.clear();
        for (String k : active_jobs_.keySet()) {
            Job j = active_jobs_.get(k);

            ArrayList<Coflow> coflows = j.get_running_coflows();
            for (Coflow c : coflows) {
                if (c.done()) {
                    c.start_timestamp_ = current_time;
                    handle_finished_coflow(c, current_time);
                }
                else {
                    System.out.println("Manager/update_and_schedule_flows: Adding coflow " + c.id_);
                    active_coflows_.put(c.id_, c);
                }
            }
        }

        // Update our set of flows
        active_flows_.clear(); // init the flows

        // create new active flows from coflows.
        HashMap<String, Flow> scheduled_flows = scheduler_.schedule_flows(active_coflows_, current_time);
        active_flows_.putAll(scheduled_flows);
    }
}
