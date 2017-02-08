package gaiasim.spark;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Flow;

// A stage represents a shuffle within a job. It is an edge within a DAG.
public class Stage {
    // TODO(jack): Once finished translating from original simulator, switch
    // naming of parent and child. It makes more sense to me to have this
    // stage be a dependent of its parent and to have child stages be
    // dependents of this stage.
    
    public String id_;

    // The location of stage-initiating tasks. For example, these would be
    // the locations of map tasks in a map-reduce shuffle.
    public String[] task_locs_;

    // Stages that this Stage depends on (must complete before this
    // Stage starts).
    public ArrayList<Stage> child_stages_ = new ArrayList<Stage>();

    // Stages which depend on this Stage (this Stage must complete
    // before parent Stages start).
    public ArrayList<Stage> parent_stages_ = new ArrayList<Stage>();

    public double volume;
    public boolean done = false;

    // The coflow representing all communication in this Stage. There
    // are assumed to be flows between all of our tasks and all of the
    // tasks of our parents. 
    public Coflow coflow_;

    public Stage(String id, String[] task_locs) {
        id_ = id;
        task_locs_ = task_locs;
    }

    public void create_coflow() {
        double total_volume = 0.0;
        HashMap<String, Flow> flows = new HashMap<String, Flow>();

        String flow_id_prefix = id_ + ":";
        int flow_id_suffix = 0;

        // This shuffle transmits data to other tasks in the DAG. Tasks are
        // grouped together into the shuffles resulting from them.
        for (Stage child : child_stages_) {

            // A child will have tasks in multiple locations. We assume that
            // there is one flow between each pair of locations within our
            // task set and the child's task set and that these transfers
            // are all of the same size. Note that flows go from
            // child_task -> our_task.
            int num_flows = task_locs_.length * child.task_locs_.length;
            double volume_per_flow = child.volume / (double)num_flows;
            for (String src_loc : child.task_locs_) {

                for (String dst_loc : task_locs_) {

                    // If the src and dst locations are the same, no network
                    // transmission is needed, so we don't create a flow.
                    if (src_loc != dst_loc) {
                        String flow_id = flow_id_prefix + flow_id_suffix;
                        flows.put(flow_id, new Flow(flow_id, id_, src_loc, dst_loc, volume_per_flow));
                        total_volume += volume_per_flow;
                    }

                } // task_locs_

            } // for child.task_locs_

        } // for child_stages_

        coflow_ = new Coflow(id_, total_volume, flows);
    }

    // Returns whether the Stage can begin or not. A Stage can begin
    // only if all of the Stages on which it depends have completed.
    public boolean ready() {
        for (Stage s : child_stages_) {
            if (!s.done) {
                return false;
            }
        }

        return true;
    }
}

