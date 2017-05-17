package gaiasim.spark;

// DAG is what is submitted by DAGReader to YARNEmulator.
// It is like "Job" class. but simpler.
// dependencies are stored in DAG rather than in Coflow this time!
// we don't make DAG backward compatible yet.

// since it is only inside YARN, so it's fine to make it public.

// Maybe add optimizations of making coflows finish immediately in case of co-location? Jimmy: not necessary, we handle this in master.

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import gaiasim.gaiamaster.Coflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DAG {
    public String id;
    public long arrivalTime;

    public long startTime;
    public long finishTime;

    public HashMap<String, Coflow> coflowlist; // we don't remove coflows from this list while executing.

    //// ******* We remove coflows from the {rootCoflowsID, to_parents } while executing.
    public Set<String> rootCoflowsID; // root Coflows := active coflows // MUST use set to guarantee uniqueness.

    // using set multimap so there will be no duplicate items.
    public SetMultimap<String , String> coflow_to_parents;
    public SetMultimap<String , String> coflow_to_children; // we also don't remove coflows from this list.

    public DAG (String id, long arrivalTime){
        this.id = id;
        this.arrivalTime = arrivalTime;
        this.rootCoflowsID = new HashSet<String>();
        this.coflow_to_parents = HashMultimap.create();
        this.coflow_to_children = HashMultimap.create();
        this.coflowlist = new HashMap<>(); // TODO check init
    }

    // handles the finish of a Coflow, and returns a list of new root Coflows -> for scheduling? FIXME!
    public ArrayList<Coflow> onCoflowFIN(String finishedCoflowID){
        // first manipulate the state of DAG.
        if(coflowlist.containsKey(finishedCoflowID)){
            if(rootCoflowsID.contains(finishedCoflowID)){
                // remove it from rootCoflows (MUST remove it from the entire DAG, so need also remove the multimap)
                rootCoflowsID.remove(finishedCoflowID);

                // search its childrens, remove { child -> parent (Coflow_FIN) }.
                for ( String child : coflow_to_children.get(finishedCoflowID) ){
                    coflow_to_parents.remove( child , finishedCoflowID  );
                    // If dependency are met, add the child coflows to root
                    if (coflow_to_parents.get(child).isEmpty()) {
                        rootCoflowsID.add(child);
                    }
                }
            }
            else {
                System.err.println("YARN: [ERROR] Received a COFLOW_FIN for a coflow that is not active (not scheduled or has finished)");
                System.exit(1);
            }
        }
        else {
            System.err.println("YARN: [ERROR] Received COFLOW_FIN for a non-existing coflow");
            System.exit(1);
        }

        // then return the root. If we are done, set the finish time.
        if(getRootCoflows().isEmpty()){
            onFinish();
            return null;
        }
        return getRootCoflows(); // FIXME!
    }

    public ArrayList<Coflow> getRootCoflows() {
        ArrayList<Coflow> ret = new ArrayList<Coflow> (rootCoflowsID.size());
        for(String k : rootCoflowsID){
            ret.add(coflowlist.get(k));
        }
        return ret;
    }

    public boolean isDone() { return rootCoflowsID.isEmpty(); }

    public void onStart(){
        startTime = System.currentTimeMillis();
    }

    public void onFinish(){
        finishTime = System.currentTimeMillis();
    }

    // Given {src, dst}, set the appropriate dependency:
    // (1) dst.parent.has(src)
    // (2) remove dst from root, add src to root.
    public void setDependency( String src_stage , String dst_stage ){
        // first set the coflow -> [] parent mapping
        coflow_to_parents.put(dst_stage , src_stage);
        // then set the coflow -> children mapping
        coflow_to_children.put(src_stage , dst_stage);
        // Then set the root_list:
        rootCoflowsID.add(src_stage); // Because it is a set this is fine!
        rootCoflowsID.remove(dst_stage);
    }


    ///// getters and setters /////
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

}
