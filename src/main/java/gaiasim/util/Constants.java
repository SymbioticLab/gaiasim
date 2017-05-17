package gaiasim.util;

import java.util.HashMap;

public class Constants {

    // Min interval between schedule
    public static final int SCHEDULE_INTERVAL_MS = 500;

    // Number of milliseconds in a second
    public static final int MILLI_IN_SECOND = 1000;
    public static final double MILLI_IN_SECOND_D = 1000.0;

    // Timestep between advancements of flow transmission
    // in milliseconds.
    public static final int SIMULATION_TIMESTEP_MILLI = 10;

    public static final double SIMULATION_TIMESTEP_SEC = (double)SIMULATION_TIMESTEP_MILLI / (double)MILLI_IN_SECOND;

    // The number of milliseconds in an epoch. An epoch is
    // a period during which jobs may be scheduled. By
    // default we place this at 1 second because we want
    // to be able to schedule jobs at the granularity of
    // one second.
    public static final int EPOCH_MILLI = MILLI_IN_SECOND / 100;

//    public static final int DEFAULT_OUTPUTSTREAM_RATE = 100000;

    // Block is the maximum size of transmission. (64MB for GB/s level trasmission)
    public static final int BLOCK_SIZE_MB = 64;

    public static final int BUFFER_SIZE = 64 * 1024 * 1024;

    public static final int DEFAULT_TOKEN_RATE = 40;

    public static HashMap<String, String> node_id_to_trace_id;

    // Return the id of the job owning the Stage, Coflow_Old, or FlowGroup
    // identified by id.
    public static String get_job_id(String id) {
        // Stage, Coflow_Old, and FlowGroup ids begin in the form <job_id>:
        return id.split(":")[0];
    }

    // Return the id of the coflow owining this flow
    public static String get_coflow_id(String id) {
        // FlowGroup ids are of the form <job_id>:<coflow_id>:<flow_id>
        String[] splits = id.split(":");
        return splits[0] + ":" + splits[1];
    }
}
