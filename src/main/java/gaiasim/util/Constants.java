package gaiasim.util;

import java.util.HashMap;

public class Constants {

    // Number of milliseconds in a second
    public static final int MILLI_IN_SECOND = 1000;
    public static final double MILLI_IN_SECOND_D = 1000.0;

    // Timestep between advancements of flow transmission
    // in milliseconds.
    public static final int SIMULATION_TIMESTEP_MILLI = 10;

    public static final double SIMULATION_TIMESTEP_SEC = (double) SIMULATION_TIMESTEP_MILLI / (double) MILLI_IN_SECOND;

    // The number of milliseconds in an epoch. An epoch is
    // a period during which jobs may be scheduled. By
    // default we place this at 1 second because we want
    // to be able to schedule jobs at the granularity of
    // one second.
    public static final int EPOCH_MILLI = MILLI_IN_SECOND / 100;
    public static final double EPSILON = 0.01;
    public static final long COLLOCATED_FG_COMPLETION_TIME = 0;
    public static final double VALID_CCT_THR = 0.00001;
    public static final double FLOW_RATE_THR = 0.1;
    public static HashMap<String, String> node_id_to_trace_id;

    // Return the id of the job owning the Stage, Coflow, or Flow
    // identified by id.
    public static String get_job_id(String id) {
        // Stage, Coflow, and Flow ids begin in the form <job_id>:
        return id.split(":")[0];
    }

    // Constants for the JCT calculation
    public static final double TASK_SIZE_MB = 256;
    public static final int CORE_COUNT = 4;
    public static final int IO_SPEED_MB_S = 50;
}
