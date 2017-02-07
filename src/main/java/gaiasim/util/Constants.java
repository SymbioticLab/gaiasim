package gaiasim.util;

public class Constants {

    // Number of milliseconds in a second
    public static final int MILLI_IN_SECOND = 1000;

    // Timestep between advancements of flow transmission
    // in milliseconds.
    public static final int SIMULATION_TIMESTEP_MILLI = 10;

    // The number of milliseconds in an epoch. An epoch is
    // a period during which jobs may be scheduled. By
    // default we place this at 1 second because we want
    // to be able to schedule jobs at the granularity of
    // one second.
    public static final int EPOCH_MILLI = MILLI_IN_SECOND;
}
