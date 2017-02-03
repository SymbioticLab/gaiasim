package gaiasim.network;

public class Flow {
    public String id_;
    public String src_loc_;
    public String dst_loc_;
    public double volume_;
    public double transmitted_;
    public double rate_;

    public Flow(String id, String src_loc, String dst_loc, double volume) {
        id_ = id;
        src_loc_ = src_loc;
        dst_loc_ = dst_loc;
        volume_ = volume;
        rate_ = (double)0.0;
        transmitted_ = (double)0.0;
    }
};
