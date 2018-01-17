package gaiasim.network;

import java.util.ArrayList;

import gaiasim.util.Constants;

public class Link {
    // The maximum bandwidth this link can serve
    public double max_bw_;
    public double cur_bw_;
    public String src_loc_;
    public String dst_loc_;
    public double original_bw_;

    private double previous_bw;

    public Link(double max_bw) {
        max_bw_ = max_bw;
        cur_bw_ = max_bw;
        original_bw_ = max_bw_;
    }

    public Link(String src_loc, String dst_loc, double max_bw) {
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        max_bw_ = max_bw;
        cur_bw_ = max_bw;
        original_bw_ = max_bw_;
    }

    public String toString() {
        return "[ " + Constants.node_id_to_trace_id.get(src_loc_) + ", " + Constants.node_id_to_trace_id.get(dst_loc_) + "] " + cur_bw_;
    }

    public double getCur_bw_() { return cur_bw_; }

    public void setCur_bw_(double cur_bw_) { this.cur_bw_ = cur_bw_; }

    public double getMax_bw_() { return max_bw_;}

    public void setMax_bw_(double max_bw_) { this.max_bw_ = max_bw_; }

    public void goDown() {
        previous_bw = max_bw_;
        max_bw_ = 0;
        cur_bw_ = 0;
    }

    // Changed this, a little hack here
    //FIXME in the future should prevent a link go down twice without going up
    public void goUp() {
        max_bw_ = original_bw_;
        cur_bw_ = original_bw_;
    }
}
