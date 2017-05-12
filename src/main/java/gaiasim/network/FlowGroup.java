package gaiasim.network;

import java.util.ArrayList;

// This is actually a FlowGroup, owned by a CoFlow.
public class FlowGroup {
    public String id_;
    public int int_id_;
    public String coflow_id_; // id of owning coflow
    public String src_loc_;
    public String dst_loc_;
    public double volume_;
    public double transmitted_volume;
    public double rate_; // in Mbps
    public ArrayList<Pathway> paths_ = new ArrayList<Pathway>();
    public boolean done_ = false;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;
    public boolean updated_ = false; // whether the flow has had its allocation updated
    public boolean started_sending_ = false; // whether the flow has started to be sent
                                             // by a sending agent (only used by baseline)

    public FlowGroup(String id, int int_id, String coflow_id, String src_loc, String dst_loc, double volume) {
        id_ = id;
        int_id_ = int_id;
        coflow_id_ = coflow_id;
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        volume_ = volume;
        rate_ = (double)0.0;
        transmitted_volume = (double)0.0;
    }

    public double remaining_volume() {
        return volume_ - transmitted_volume;
    }
};
