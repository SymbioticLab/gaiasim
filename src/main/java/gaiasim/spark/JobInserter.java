package gaiasim.spark;

import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.ScheduleMessage;

public class JobInserter implements Runnable {
    public Vector<Job> jobs_by_time_;
    public LinkedBlockingQueue<ScheduleMessage> insert_queue_;

    private Vector<String> batchedJobs;

    public JobInserter(Vector<Job> jobs_by_time,
                       LinkedBlockingQueue<ScheduleMessage> insert_queue) {
        jobs_by_time_ = jobs_by_time;
        insert_queue_ = insert_queue;
        this.batchedJobs = new Vector<String>();
    }
    
    public void run() {
        long time_sleep, start, end;
        long last_job_time = 0;
        System.out.println("JobInserter: Started.");
        for (Job j : jobs_by_time_) {
            time_sleep = j.start_time_ - last_job_time;

            if(time_sleep == 0){ // this job is in the same flush as the previous batch
                batchedJobs.add(j.id_);
            }
            else { // flush the previous job, and add to empty batch pool
                flushJobs();
                batchedJobs.add(j.id_);
            }

            System.out.println("JobInserter: Waiting " + time_sleep + " ms before inserting new job " + j.id_);
            start = System.currentTimeMillis();
            while (time_sleep > 0) {
                try {
                    Thread.sleep(time_sleep);
                    break;
                }
                catch (InterruptedException e) {
                    end = System.currentTimeMillis();
                    time_sleep -= (end - start);
                }
            } // while time_sleep > 0
            
            last_job_time = j.start_time_;

//            try {
//
////                insert_queue_.put(new ScheduleMessage(ScheduleMessage.Type.JOB_INSERTION, j.id_));
//                // batch job at job inserter!
//
////                System.out.println("JobInserter: Inserted job " + j.id_);
//            }
//            catch (InterruptedException e) {
//                // We shouldn't ever get this
//                e.printStackTrace();
//                System.exit(1);
//            }

        } // for jobs

        flushJobs(); // flush at the end of all jobs

    }

    private void flushJobs(){
        System.out.println("JobInserter: adding " + batchedJobs);

        try {
            insert_queue_.put(new ScheduleMessage( batchedJobs));

            batchedJobs.clear();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
