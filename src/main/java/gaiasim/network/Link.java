package gaiasim.network;

import java.util.ArrayList;

import gaiasim.network.Flow;

public class Link {
    // The maximum bandwidth this link can serve
    public double max_bw_;

    // Flows which are currently using this link
    public ArrayList<Flow> subscribers_ = new ArrayList<Flow>();

    public Link(double max_bw) {
        max_bw_ = max_bw;
    }

    // Return the amount of bandwidth allocated to each subscribing
    // flow if all subscribers receive an equal amount.
    public double bw_per_flow() {
        return subscribers_.isEmpty() ? max_bw_ : max_bw_ / (double)subscribers_.size();
    }

    // Return the amount of bandwidth not yet allocated.
    public double remaining_bw() {
        double remaining_bw = max_bw_;
        for (Flow f : subscribers_) {
            remaining_bw -= f.rate_;
        }

        return remaining_bw;
    }
}
