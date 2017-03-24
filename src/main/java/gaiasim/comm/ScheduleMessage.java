package gaiasim.comm;

public class ScheduleMessage {
    public enum Type {
        JOB_INSERTION,
        FLOW_COMPLETION,
        FLOW_STATUS_RESPONSE
    }
    
    public Type type_;
    public String job_id_;      // Used only by JOB_INSERTION
    public String flow_id_;     // Used by FLOW_COMPLETION and FLOW_STATUS_RESPONSE
    public String coflow_id_;   // Used by FLOW_COMPLETION and FLOW_STATUS_RESPONSE
    public long transmitted_;   // Used only by FLOW_STATUS_RESPONSE

    // For constructing JOB_INSERTION messages
    public ScheduleMessage(Type type, String job_id) {
        type_ = type;
        job_id_ = job_id;
    }

    // For constructing FLOW_COMPLETION messages
    public ScheduleMessage(Type type, String flow_id, 
                           String coflow_id) {
        type_ = type;
        flow_id_ = flow_id;
        coflow_id_ = coflow_id;
    }

    // For constructing FLOW_STATUS_RESPONSE messages
    public ScheduleMessage(Type type, String flow_id, 
                           String coflow_id, long transmitted) {
        type_ = type;
        flow_id_ = flow_id;
        coflow_id_ = coflow_id;
        transmitted_ = transmitted;
    }

}

