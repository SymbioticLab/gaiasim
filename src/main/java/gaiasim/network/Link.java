package gaiasim.network;

import java.util.ArrayList;

import gaiasim.network.Flow;

public class Link {
    // The maximum bandwidth this link can serve
    public double max_bw_;
    public double cur_bw_;
    public String src_loc_;
    public String dst_loc_;

    public Link(double max_bw) {
        max_bw_ = max_bw;
        cur_bw_ = max_bw;
    }

    public Link(String src_loc, String dst_loc, double max_bw) {
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        max_bw_ = max_bw;
    }
}
