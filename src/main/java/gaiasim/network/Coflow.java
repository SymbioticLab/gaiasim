package gaiasim.network;

// ver 1.1 fixed naming of parent_coflows and child_coflows

import java.util.ArrayList;
import java.util.HashMap;

// A coflow represents a shuffle within a job. It is an edge within a DAG.
public class Coflow {
    
    public String id_;
    public HashMap<String, Flow> flows_ = new HashMap<String, Flow>();
    public double volume_ = 0.0;
    public long start_timestamp_ = -1;
    public long end_timestamp_ = -1;
    public boolean done_ = false;

    // The location of coflow-initiating tasks. For example, these would be
    // the locations of map tasks in a map-reduce shuffle.
    public String[] task_locs_;

    // Coflows that this coflow depends on (must complete before this
    // coflow starts).
    public ArrayList<Coflow> parent_coflows = new ArrayList<Coflow>();

    // Coflows which depend on this Coflow (this Coflow must complete
    // before child Coflows start).
    public ArrayList<Coflow> child_coflows = new ArrayList<Coflow>();

    // The volume to be shuffled to parent coflow, keyed by parent coflow id
    // This is not used in the new version
    public HashMap<String, Double> volume_for_parent_ = new HashMap<String, Double>();

    public Coflow(String id, String[] task_locs) {
        id_ = id;
        task_locs_ = task_locs;
    }

    public void create_flows() {
        volume_ = 0.0;

        String flow_id_prefix = id_ + ":";
        int flow_id_suffix = 0;

        // This shuffle transmits data to other tasks in the DAG. Tasks are
        // grouped together into the shuffles resulting from them.
        for (Coflow parent : parent_coflows) {

            // A child will have tasks in multiple locations. We assume that
            // there is one flow between each pair of locations within our
            // task set and the child's task set and that these transfers
            // are all of the same size. Note that flows go from
            // child_task -> our_task.
            int num_flows = task_locs_.length * parent.task_locs_.length;
            double volume_per_flow = parent.volume_for_parent_.get(id_) / (double)num_flows;
            for (String src_loc : parent.task_locs_) {

                for (String dst_loc : task_locs_) {

                    // If the src and dst locations are the same, no network
                    // transmission is needed, so we don't create a flow.
                    if (src_loc != dst_loc) {
                        String flow_id = flow_id_prefix + flow_id_suffix;
                        flows_.put(flow_id, new Flow(flow_id, flow_id_suffix, id_, src_loc, dst_loc, volume_per_flow));
                        volume_ += volume_per_flow;
                        flow_id_suffix++;
                    }
                    else {
                        System.out.println("Skipping because src and dst are same " + src_loc + " " + dst_loc + " " + parent.id_ + "->" + id_);
                    }

                } // task_locs_

            } // for child.task_locs_

        } // for parent_coflows

    }

    // Sets the coflow's start time to be that of the earliest starting flow.
    // Assumes all flows are done.
    public void determine_start_time() {
        start_timestamp_  = Long.MAX_VALUE;
        for (String k : flows_.keySet()) {
            Flow f = flows_.get(k);
            if (f.start_timestamp_ < start_timestamp_) {
                start_timestamp_ = f.start_timestamp_;
            }
        }
    }

    // Return whether owned Flows are done
    public boolean done() {
        if (!done_) {
            for (String k : flows_.keySet()) {
                if (!flows_.get(k).done_) {
                    return false;
                }
            }
            done_ = true;
        }
        return true;
    }

    // Returns whether the Coflow can begin or not. A Coflow can begin
    // only if all of the Coflows on which it depends have completed.
    public boolean ready() {
        for (Coflow s : parent_coflows) {
            if (!s.done_) {
                return false;
            }
        }

        return true;
    }
}

