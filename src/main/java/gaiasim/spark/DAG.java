package gaiasim.spark;

// CoflowDAG is what is submitted by DAGReader to YARNEmulator.
// This DAG is NOT the same as the DAG in the trace, this is DAG between Coflows!!!
// Dependencies are stored in DAG rather than in Coflow this time!

// There are many ways to transform a job DAG into dependency graph of Coflows.
// The one we use in DAGReader is only one valid way.
// Key idea: construct coflows for dst in a shuffle (src,dst). So coflows are defined by collecting data to one sink.

// since it is only inside YARN, so it's fine to make the field public.
// we don't make DAG backward compatible yet.
// Maybe add optimizations of making coflows finish immediately in case of co-location? Jimmy: not necessary, we handle this in master.

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import gaiasim.gaiamaster.Coflow;
import gaiasim.gaiamaster.FlowGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DAG {
    public String id;
    public long arrivalTime;

    public long startTime;
    public long finishTime;

    public HashMap<String, Coflow> coflowList; // we don't remove coflows from this list while executing.

    //// ******* We remove coflows from the {rootCoflowsID, to_parents } while executing.
    public Set<String> rootCoflowsID; // root Coflows := active coflows // MUST use set to guarantee uniqueness.


    // using SetMultimap so there will be no duplicate items.
    // This is actually mapping between stages, so contains more than mapping between coflows.
    public SetMultimap<String , String> coflow_to_parents;
    public SetMultimap<String , String> coflow_to_children; // we also don't remove coflows from this list.

    public DAG (String id, long arrivalTime){
        this.id = id;
        this.arrivalTime = arrivalTime;
        this.rootCoflowsID = new HashSet<String>();
        this.coflow_to_parents = HashMultimap.create();
        this.coflow_to_children = HashMultimap.create();
        this.coflowList = new HashMap<>(); // TODO check init
    }

    // handles the finish of a Coflow, and returns a list of new root Coflows, for scheduling (ONLY new ones!)
    public ArrayList<Coflow> onCoflowFIN(String finishedCoflowID){
        ArrayList<Coflow> ret = new ArrayList<Coflow>();
        // first manipulate the state of DAG.
        if(coflowList.containsKey(finishedCoflowID)){
            if(rootCoflowsID.contains(finishedCoflowID)){
                // remove it from rootCoflows (MUST remove it from the entire DAG, so need also remove the multimap)
                rootCoflowsID.remove(finishedCoflowID);

                // search its childrens, remove { child -> parent (Coflow_FIN) }.
                for ( String child : coflow_to_children.get(finishedCoflowID) ){
                    coflow_to_parents.remove( child , finishedCoflowID  );
                    // If dependency are met, add the child coflows to root
                    if (coflow_to_parents.get(child).isEmpty()) {
                        rootCoflowsID.add(child);
                        ret.add( coflowList.get(child) );
                    }
                }
            }
            else {
                System.err.println("YARN: [ERROR] Received a COFLOW_FIN for a coflow that is not active (not scheduled or has finished)");
                System.exit(1);
            }
        }
        else {
            System.err.println("YARN: [ERROR] Received COFLOW_FIN for a non-existent coflow");
            System.exit(1);
        }

        // TODO check the logic here.
        // then return the root. If we are done, set the finish time.
        if(getRootCoflows().isEmpty()){
            onFinish();
            return null;
        }
        return ret;
    }

    public ArrayList<Coflow> getRootCoflows() {
        ArrayList<Coflow> ret = new ArrayList<Coflow> (rootCoflowsID.size());
        for(String k : rootCoflowsID){
            ret.add(coflowList.get(k));
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
    // (2) src.child.has(dst)
    public void updateDependency(String src_stage , String dst_stage ){
        // first set the coflow -> [] parent mapping
        coflow_to_parents.put(dst_stage , src_stage);
        // then set the coflow -> children mapping
        coflow_to_children.put(src_stage , dst_stage);

        // update the rootSet to contain the root of stages (not coflows), but their childrens are root coflows.
        rootCoflowsID.add(src_stage);
        rootCoflowsID.remove(dst_stage);
    }


    // construct a list of coflow from the Multimap
    public void addCoflows(ArrayListMultimap<String, FlowGroup> tmpCoflowList) {
        for (String coflowID : tmpCoflowList.keySet()){
            Coflow cf = new Coflow( coflowID , (ArrayList<FlowGroup>) tmpCoflowList.get(coflowID)); // TODO: verify
            coflowList.put( coflowID , cf);
        }
    }

    // TODO for all stages, trim those that do not delegate a coflow, and mark their children coflow as root
    // we can ONLY do this after we are done with all coflowList and dependencies!
    public void trimRoot() {
        // TODO: verify this.
        for ( String k : rootCoflowsID){
            // search its childrens, remove { child -> parent (k) }.
            for ( String child : coflow_to_children.get(k) ){
                coflow_to_parents.remove( child , k  );
                // If dependency are met, add the child coflows to root
                if (coflow_to_parents.get(child).isEmpty()) {
                    rootCoflowsID.add(child);
                }
            }
        }
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
