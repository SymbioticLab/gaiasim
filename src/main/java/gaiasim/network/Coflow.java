package gaiasim.network;

import java.util.ArrayList;
import java.util.HashMap;

// A coflow represents a shuffle within a job. It is an edge within a DAG.
public class Coflow {
    // TODO(jack): Once finished translating from original simulator, switch
    // naming of parent and child. It makes more sense to me to have this
    // stage be a dependent of its parent and to have child stages be
    // dependents of this stage.
    
    private String id;
    public HashMap<String, FlowGroup> flows = new HashMap<String, FlowGroup>();
    private double volume = 0.0;
    private long start_timestamp = -1;
    private long end_timestamp = -1;
    private boolean done = false;

    // The location of coflow-initiating tasks. For example, these would be
    // the locations of map tasks in a map-reduce shuffle.
    public String[] task_locs;

    // Coflows that this coflow depends on (must complete before this
    // coflow starts).
    public ArrayList<Coflow> child_coflows = new ArrayList<Coflow>();

    // Coflows which depend on this Coflow (this Coflow must complete
    // before parent Coflows start).
    public ArrayList<Coflow> parent_coflows = new ArrayList<Coflow>();

    // The volume to be shuffled to parent coflow, keyed by parent coflow id
    public HashMap<String, Double> volume_for_parent = new HashMap<String, Double>();

    public Coflow(String id, String[] task_locs) {
        this.id = id;
        this.task_locs = task_locs;
    }

    public void create_flows() {
        volume = 0.0;

        String flow_id_prefix = id + ":";
        int flow_id_suffix = 0;

        // This shuffle transmits data to other tasks in the DAG. Tasks are
        // grouped together into the shuffles resulting from them.
        for (Coflow child : child_coflows) {

            // A child will have tasks in multiple locations. We assume that
            // there is one flow between each pair of locations within our
            // task set and the child's task set and that these transfers
            // are all of the same size. Note that flows go from
            // child_task -> our_task.
            int num_flows = task_locs.length * child.task_locs.length;
            double volume_per_flow = child.volume_for_parent.get(id) / (double)num_flows;
            for (String src_loc : child.task_locs) {

                for (String dst_loc : task_locs) {

                    // If the src and dst locations are the same, no network
                    // transmission is needed, so we don't create a flow.
                    if (src_loc != dst_loc) {
                        String flow_id = flow_id_prefix + flow_id_suffix;
                        flows.put(flow_id, new FlowGroup(flow_id, flow_id_suffix, id, src_loc, dst_loc, volume_per_flow));
                        System.out.println("CoFlow: created flow id: " + flow_id + " owned by coflow id: " + id + " with volume " + volume_per_flow);
                        volume += volume_per_flow;
                        flow_id_suffix++;
                    }
                    else {
                        System.out.println("Skipping because src and dst are same " + src_loc + " " + dst_loc + " " + child.id + "->" + id);
                    }

                } // task_locs

            } // for child.task_locs

        } // for child_coflows

    }

    // Sets the coflow's start time to be that of the earliest starting flow.
    // Assumes all flows are done.
    public void determine_start_time() {
        // TODO: what if there are no flows. creating a new function.
        start_timestamp = Long.MAX_VALUE;
        for (String k : flows.keySet()) {
            FlowGroup f = flows.get(k);
            if (f.getStart_timestamp() < start_timestamp) {
                start_timestamp = f.getStart_timestamp();
            }
        }
    }

    // TODO: recapsuling the CoFlow Class and provide the proper interface.
    public long getCoFlowStartTime() {
        return 0;
    }

    // Return whether owned Flows are done
    public boolean done() {
        if (!done) {
            for (String k : flows.keySet()) {
                if (!flows.get(k).isDone()) {
                    return false;
                }
            }
            done = true;
        }
        return true;
    }

    // Returns whether the Coflow can begin or not. A Coflow can begin
    // only if all of the Coflows on which it depends have completed.
    public boolean ready() {
        for (Coflow s : child_coflows) {
            if (!s.done) {
                return false;
            }
        }

        return true;
    }


    ///// getters and setters /////

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public long getStart_timestamp() {
        return start_timestamp;
    }

    public void setStart_timestamp(long start_timestamp) {
        this.start_timestamp = start_timestamp;
    }

    public long getEnd_timestamp() {
        return end_timestamp;
    }

    public void setEnd_timestamp(long end_timestamp) {
        this.end_timestamp = end_timestamp;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }


}

