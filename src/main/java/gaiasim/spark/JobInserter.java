package gaiasim.spark;

import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import gaiasim.comm.ScheduleMessage;
import gaiasim.spark.Job;

public class JobInserter implements Runnable {
    public Vector<Job> jobs_by_time_;
    public LinkedBlockingQueue<ScheduleMessage> insert_queue_;

    public JobInserter(Vector<Job> jobs_by_time,
                       LinkedBlockingQueue<ScheduleMessage> insert_queue) {
        jobs_by_time_ = jobs_by_time;
        insert_queue_ = insert_queue;
    }
    
    public void run() {
        long time_sleep, start, end;
        long cur_time = 0;
        for (Job j : jobs_by_time_) {
            time_sleep = j.start_time_ - cur_time;
            System.out.println("JobInserter: Waiting " + time_sleep + " before inserting new job " + j.id_);
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
            
            cur_time = j.start_time_;

            try {
                insert_queue_.put(new ScheduleMessage(ScheduleMessage.Type.JOB_INSERTION, j.id_));
                System.out.println("JobInserter: Inserted job " + j.id_);
            }
            catch (InterruptedException e) {
                // We shouldn't ever get this
                e.printStackTrace();
                System.exit(1);
            }

        } // for jobs
    }

}
