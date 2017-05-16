package gaiasim.spark;

// DAG is what is submitted by DAGReader to YARNEmulator.
// It is like "Job" class. but simpler.
// dependencies are stored in DAG rather than in Coflow this time!
// we don't make DAG backward compatible yet.

// since it is only inside YARN, so it's fine to make it public.

// TODO: add optimizations of making coflows finish immediately in case of co-location.

import gaiasim.gaiamaster.Coflow;
import gaiasim.network.Coflow_Old;

import java.util.ArrayList;
import java.util.HashMap;

public class DAG {
    public String id;
    public long arrivalTime;
    public long startTime;
    public long finishTime;

    public HashMap<String, Coflow> coflowlist;

    public ArrayList<Coflow> rootCoflows; // root Coflows := active coflows
    public HashMap<Coflow , ArrayList<Coflow>> coflow_to_childrens;

    public DAG (String id, long arrivalTime){
        this.id = id;
        this.arrivalTime = arrivalTime;



    }

    // handles the finish of a Coflow, and returns a list of new root Coflows.
    public ArrayList<Coflow> onCoflowFIN(String finishedCoflowID){
        // first manipulate the state of DAG.
        if(coflowlist.containsKey(finishedCoflowID)){
            Coflow cf = coflowlist.get(finishedCoflowID);
            if(rootCoflows.contains(cf)){
                // then we should add the child coflows to root
                rootCoflows.addAll( coflow_to_childrens.get(cf) );

                // then we can remove the finishing coflows from FIN.
                rootCoflows.remove(cf);
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
        return getRootCoflows();
    }

    public ArrayList<Coflow> getRootCoflows(){ return rootCoflows; }

    public boolean isDone() { return rootCoflows.isEmpty(); }

    public void onStart(){
        startTime = System.currentTimeMillis();
    }

    public void onFinish(){
        finishTime = System.currentTimeMillis();
    }




//
//    public HashMap<String, Coflow_Old> coflows = new HashMap<String, Coflow_Old>();
//    public ArrayList<Coflow_Old> root_coflows = new ArrayList<Coflow_Old>();
//    public ArrayList<Coflow_Old> leaf_coflows = new ArrayList<Coflow_Old>();
//    private boolean started = false;
//    private long start_timestamp = -1;
//    private long end_timestamp = -1;
//
//    // Coflows that are currently running
//    public ArrayList<Coflow_Old> running_coflows = new ArrayList<Coflow_Old>();
//
//    // Coflows taht are ready to begin but have not begun yet
//    public ArrayList<Coflow_Old> ready_coflows = new ArrayList<Coflow_Old>();

//    public DAG(String id, long start_time, HashMap<String, Coflow_Old> coflows) {
//        this.id = id;
//        this.coflows = coflows;
//        arrivalTime = start_time;
//
//        // Determine the end coflows of the DAG (those without any parents).
//        // Determine the start coflows of the DAG (those without children).
//        for (String key : this.coflows.keySet()) {
//            Coflow_Old c = this.coflows.get(key);
//            if (c.parent_coflows.size() == 0) {
//                leaf_coflows.add(c);
//            }
//
//            if (c.child_coflows.size() == 0) {
//                root_coflows.add(c);
//            }
//            else {
//                // Flows are created by depedent coflows. Since
//                // starting coflows do not depend on anyone, they
//                // should not create coflows.
//                c.create_flows();
//            }
//        }
//    }

//    // A job is considered done if all of its coflows are done
//    public boolean done() {
//        for (String k : coflows.keySet()) {
//            Coflow_Old c = coflows.get(k);
//            if (!(c.done() || c.flows.isEmpty())) {
//                return false;
//            }
//        }
//
//        return true;
//    }



//    // Start all of the first coflows of the job
//    public void start() {
//        for (Coflow_Old c : root_coflows) {
//            c.setDone(true);
//            // Coflows are defined from parent stage to child stage,
//            // so we add the start stage's parents first.
//            for (Coflow_Old parent : c.parent_coflows) {
//                if (!ready_coflows.contains(parent)) {
//                    if (parent.done()) {
//                        // Hack to avoid error in finish_coflow
//                        running_coflows.add(parent);
//                        finish_coflow(parent.getId());
//                    }
//                    // Add coflows which can be scheduled as a whole
//                    else if (parent.ready()) {
//                        ready_coflows.add(parent);
//                    }
//                }
//            } // for parent_coflows
//
//        } // for root_coflows
//        started = true;
//    }

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
