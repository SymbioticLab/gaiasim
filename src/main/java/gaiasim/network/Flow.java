package gaiasim.network;

import java.util.ArrayList;

public class Flow {
    public String id_;
    public int int_id_;
    public String coflow_id_; // id of owning coflow
    public String src_loc_;
    public String dst_loc_;
    public double volume_;
    public double transmitted_;

    public String getDst_loc_() {
        return dst_loc_;
    }

    public double rate_; // in Mbps
    public ArrayList<Pathway> paths_ = new ArrayList<>();
    public Pathway max_bw_path = null;
    public boolean done_ = false;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;

    // Reference to owning coflow used in the DarkScheduler
    // Populated in DependencyResolver.addCoflows()
    public Coflow owning_coflow_;
    public boolean scheduled_alone = false;

    public Flow(String id, int int_id, String coflow_id, String src_loc, String dst_loc, double volume) {
        id_ = id;
        int_id_ = int_id;
        coflow_id_ = coflow_id;
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        volume_ = volume;
        rate_ = 0.0;
        transmitted_ = 0.0;
    }

    public double remaining_volume() {
        return volume_ - transmitted_;
    }

    public String paths_toString() {
        if (paths_.size() == 0) {
            return "NULL";
        }

        StringBuilder ret = new StringBuilder("Paths: ");

        for (Pathway p : paths_){
            ret.append(p).append("; ");
        }

        return ret.toString();
    }
}
