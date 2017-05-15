package gaiasim.comm;

import java.io.Serializable;
import java.util.Vector;

public class ScheduleMessage implements Serializable {



    public enum Type {
        JOB_INSERTION,
        FLOW_COMPLETION,
        FLOW_STATUS_RESPONSE
    }
    
    public Type type_;
    public String job_id_;      // Used only by JOB_INSERTION
    public String flow_id_;     // Used by FLOW_COMPLETION and FLOW_STATUS_RESPONSE
    public double transmitted_; // Used only by FLOW_STATUS_RESPONSE

    public Vector<String> jobs;

    // this is only for job insertion
    public ScheduleMessage(Vector<String> batchedJobs) {

        this.type_ = Type.JOB_INSERTION;
        this.jobs = (Vector<String>) batchedJobs.clone();

    }

    // For constructing only for FLOW_COMPLETION messages
    public ScheduleMessage(Type type, String id) {
        type_ = type;
        if (type == Type.JOB_INSERTION) {
            job_id_ = id;
        }
        else {
            flow_id_ = id;
        }
    }

    // For constructing FLOW_STATUS_RESPONSE messages
    public ScheduleMessage(Type type, String flow_id, double transmitted) {
        type_ = type;
        flow_id_ = flow_id;
        transmitted_ = transmitted;
    }

}

