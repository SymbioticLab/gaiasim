package gaiasim.JCTCalc;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.spark.DAGReader;
import gaiasim.spark.Job;
import gaiasim.util.Constants;

import java.io.*;
import java.util.*;

@SuppressWarnings("Duplicates")

public class JCTCalc {

    private final int num_computers;
    private HashMap<String, Job> jobs_;
    private NetGraph net_graph_;
    private final String outdir_;

    ArrayList<Job> completed_jobs_ = new ArrayList<>();
    ArrayList<Coflow> completed_coflows_ = new ArrayList<>();
    ArrayList<Job> jobs_by_time_ = new ArrayList<>();

    HashMap<String, CCTInfo> cctInfoHashMap = new HashMap<>();
    Queue<Coflow> toBeScheduledQueue = new LinkedList<>();

    private int count = 0;
    private long dev = 0;

    public JCTCalc(String gml_file, String trace_file, String outdir, String csv, String nc) {
        this.num_computers = Integer.parseInt(nc);
        this.outdir_ = outdir;

        try {
            this.net_graph_ = new NetGraph(gml_file, 1);
            jobs_ = DAGReader.read_trace_new(trace_file, net_graph_, 1);
            parseCCTCSV(csv);

            // Create sorted vector of jobs
            for (String id : jobs_.keySet()) {
                jobs_by_time_.add(jobs_.get(id));
            }
            Collections.sort(jobs_by_time_, new Comparator<Job>() {
                public int compare(Job o1, Job o2) {
                    if (o1.start_time_ == o2.start_time_) return 0;
                    return o1.start_time_ < o2.start_time_ ? -1 : 1;
                }
            });


        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    public void calc() {

        // traverse the jobs and get the jct for each job
        for (Job j : jobs_by_time_){
            processJob(j);

        }
    }

    private void processJob(Job j){

        // iterate through CFs
        // coflow is not trimmed by the Coflow inserter, good
        long jobStartTime = -1;
        long jobEndTime = -1;

        for ( Coflow cf : j.start_coflows_){

            toBeScheduledQueue.add(cf);
            jobStartTime = (long) (cctInfoHashMap.get(cf.id_).getStartTime()*1000);

            cf.start_timestamp_ = (long) (cctInfoHashMap.get(cf.id_).getStartTime()*1000) +
                    getRootCF_ComputationWaitTime_Millis(cf);

        }

        j.start_timestamp_ = jobStartTime;

        while ( !toBeScheduledQueue.isEmpty()){

            Coflow cf = toBeScheduledQueue.remove();

            if (j.start_coflows_.contains(cf)){ // if this is starting coflow
                cf.end_timestamp_ = cf.start_timestamp_ +
                                    (long) (cctInfoHashMap.get(cf.id_).getDuration()*1000);
            } else {
                cf.end_timestamp_ = cf.start_timestamp_ +
                        (long) (cctInfoHashMap.get(cf.id_).getDuration()*1000);// +
//                        getCF_ComputationTime_Millis(cf) + getNonRootCF_WaitTime_Millis(cf);
            }

            jobEndTime = cf.end_timestamp_ + getCF_ComputationTime_Millis(cf);

            completed_coflows_.add(cf);
            j.finish_coflow(cf.id_);
            cf.done_ = true;

            for(Coflow child : cf.child_coflows){
                boolean canSchedule = true;
                for( Coflow parent_of_child : child.parent_coflows){
                    if (!parent_of_child.done()){
                        canSchedule = false;
                    }
                }

                if (canSchedule){
                    // add the children Coflow, set the startTime to its parent's endTime
//                  child.start_timestamp_ = (long) (cctInfoHashMap.get(cf.id_).getEndTime()*1000);
                    child.start_timestamp_ = cf.end_timestamp_ + getCF_ComputationTime_Millis(cf) + getNonRootCF_WaitTime_Millis(child);

                    toBeScheduledQueue.add(child);

//                    long orig_startTime = (long) (cctInfoHashMap.get(child.id_).getStartTime() * 1000);
//                    long delta = orig_startTime - child.start_timestamp_;
//                    if (delta != 0){
//                        count ++;
//                        dev += delta;
//                        System.out.println(delta);
//                    }
                }
            }
        }

        j.end_timestamp_ = jobEndTime;
        completed_jobs_.add(j);

        try {
            print_statistics("/tmp_job.csv", "/tmp_cct.csv");
        } catch (IOException e) {
            e.printStackTrace();
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

    private void parseCCTCSV(String csvFile) throws IOException {

        File file = new File(csvFile);
        FileReader fReader = new FileReader(file);
        CSVReader csvReader = new CSVReader(fReader);

        String[] header = csvReader.readNext();

        List<String[]> content = csvReader.readAll();

        for (String [] line : content){
            cctInfoHashMap.put(line[0], new CCTInfo(line[1] , line[2] , line[3]));
        }
    }

    //
    private double getComputationTime(double dataSize_MB, int num_computers){

        // num_cores 4
        // IO speed : 50MB/s

        int num_tasks = (int) Math.floor(dataSize_MB/Constants.TASK_SIZE_MB);
        int remaining_tasks = num_tasks % (num_computers * Constants.CORE_COUNT);
        int length_task_queue = num_tasks / num_computers / Constants.CORE_COUNT;
        if (remaining_tasks != 0){ // after fully scheduled (length_task_queue) slots, we still need a full slot

            return (length_task_queue + 1) * Constants.TASK_SIZE_MB / Constants.IO_SPEED_MB_S;

        }
        else {
            double remainingData_MB = dataSize_MB - length_task_queue * num_computers * Constants.CORE_COUNT * Constants.TASK_SIZE_MB;

            return length_task_queue * Constants.TASK_SIZE_MB / Constants.IO_SPEED_MB_S + remainingData_MB / Constants.IO_SPEED_MB_S;
        }

    }

    // This is for calculating the computation time for the non-root coflows.
    // Assuming each coflow are independent, the result is only relevant to data size, num_computers...
    private long getCF_ComputationTime_Millis(Coflow cf){

        double total_data = 0;
        for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()){
            total_data += fe.getValue().volume_;
        }

        return (long) getComputationTime(total_data, num_computers) * 1000; // cast to milliseconds

    }

    // This is for calculating the computation time for the root coflows.
    // Assuming each coflow are independent, the result is only relevant to data size, num_computers...
    // TODO: in reality they will compete for computation resource
    private long getRootCF_ComputationWaitTime_Millis(Coflow cf){

        double maxData = 0;
        for (Map.Entry<String, Flow> fe : cf.flows_.entrySet()){
            maxData = (maxData > fe.getValue().volume_)? maxData : fe.getValue().volume_ ;
        }

        return (long) getComputationTime(maxData, num_computers) * 1000; // cast to milliseconds

    }

    // in reality, not all coflow can start right after its parents finish with gaia (not enough bandwidth, gaia will delay many coflows).
    // we delay by the same amount of time in the simulation
    private long getNonRootCF_WaitTime_Millis(Coflow cf){

        long maxParentEndTime = 0;
        for( Coflow parent_of_child : cf.parent_coflows){
            if (!parent_of_child.done()){
                System.err.println("ERROR: parent has not finished");
                continue;
            }

            long parentEndTime = (long) (cctInfoHashMap.get(parent_of_child.id_).getEndTime() * 1000);

            maxParentEndTime = (maxParentEndTime > parentEndTime) ? maxParentEndTime : parentEndTime;
        }

//        System.out.println( (long) (cctInfoHashMap.get(cf.id_).getStartTime()*1000) - maxParentEndTime );

        return (long) (cctInfoHashMap.get(cf.id_).getStartTime()*1000) - maxParentEndTime;
    }
}
