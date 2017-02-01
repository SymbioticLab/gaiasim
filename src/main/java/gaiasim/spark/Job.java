package gaiasim.spark;

import gaiasim.network.Coflow;

public class Job {
    public String id;
    public float start_time;
    public Coflow[] coflows;
    public int cur_coflow_idx = 0;

    public Job(String id_, float start_time_) {
        id = id_;
        start_time = start_time_;
    }
}
