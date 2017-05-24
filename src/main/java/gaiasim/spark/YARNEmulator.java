package gaiasim.spark;

// This emulates a YARN, it takes in trace.txt, and outputs Coflows.
// A thread DAGReader reads the trace.txt and insert DAG at defined arrival time.
// A event loop processes the DAG_ARRIVAL and COFLOW_FIN.
// It maintains the state of active coflows and send coflows:
//  (1) insert initial Coflows w/o dependencies at the trace-specified time
//  (2) dependent coflows when their dependencies have already been met (i.e. on COFLOW_FIN)

import gaiasim.gaiamaster.Coflow;
import gaiasim.gaiamaster.FlowGroup;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class YARNEmulator implements Runnable {

    private String tracefile;
    private NetGraph netGraph;
    private LinkedBlockingQueue<YARNMessages> yarnEventQueue;
    private LinkedBlockingQueue<Coflow> coflowOutput;
    private Thread dagThread;

    private HashMap<String , DAG> dagPool; // In YARNEmulator, we define CoflowID to be DAG:dst_stage

    @Override
    public void run() {

        System.out.println("YARN: YARM Emulator is up");

        // when states are ready, start inserting jobs!
                dagThread.start();

        while (true){
            try {
                YARNMessages m = yarnEventQueue.take();
                switch (m.getType()){

                    case COFLOW_FIN:
                        // check and insert the child Coflows
                        onCoflowFIN(m.FIN_coflow_ID, System.currentTimeMillis());

                        break;

                    case DAG_ARRIVAL:
                        // insert the root Coflows

                        onDAGArrival(m.arrivedDAG);
                        break;

                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public YARNEmulator(String tracefile, NetGraph netGraph ,
            LinkedBlockingQueue<YARNMessages> yarnEventInput, LinkedBlockingQueue<Coflow> coflowOutput) {
        this.tracefile = tracefile;
        this.netGraph = netGraph;
        this.coflowOutput = coflowOutput;
        this.yarnEventQueue = yarnEventInput;
        this.dagPool = new HashMap<>();

        // init the YARN, read the trace and prepare a list of DAGs.
        dagThread = new Thread(new DAGReader(tracefile , netGraph , yarnEventQueue));

    }

    // TODO Handle finish of a coflow.
    private void onCoflowFIN(String fin_coflow_id , long timeStamp) throws InterruptedException {
        System.out.println("YARN: Received FIN for Coflow " + fin_coflow_id);
        // get the owning DAG from dag_pool , by Coflow_id.
        String [] split = fin_coflow_id.split(":"); // DAG_ID = split[0]
        if (dagPool.containsKey(split[0])){
            DAG dag = dagPool.get(split[0]);
            // set the CF completion time for this CF
            if(dag.coflowList.containsKey(fin_coflow_id)){
                dag.coflowList.get(fin_coflow_id).setEndTime(timeStamp);
            }
            else {
                System.err.println("Received FIN for Coflow that is not in the owning DAG.");
            }

            // process CF_FIN message.
            ArrayList<Coflow> cfToAdd = dag.onCoflowFIN(fin_coflow_id);

            // Check if DAG is done first, if DAG is done, handle it and return.
            // check before inserting new CF, because the insertion of CF may cause DAG to finish, if the CF is totally co-located.
            // If we insert CF first, then we will have multiple DAG_FIN.
            if (dag.isDone()){
                onDAGFinish(dag , System.currentTimeMillis());
                return;
            }

            // Only if the DAG is not done, get new coflows and schedule them
            for( Coflow cf : cfToAdd){
                insertCoflow(cf);
            }
        }
        else {
            System.err.println("YARN: Received FIN for Coflow that is not in DAG Pool.");
        }
    }

    // Handle submission of DAGs.
    private void onDAGArrival(DAG arrivedDAG) throws InterruptedException {
        System.out.println("YARN: DAG " + arrivedDAG.getId() + " arrived at " + arrivedDAG.getArrivalTime() + " s.");
        arrivedDAG.onStart();
        // first add dag to the pool.
        dagPool.put(arrivedDAG.getId() , arrivedDAG);

        // then insert the root coflows to GAIA (into coflowOutput)
        for ( Coflow cf : arrivedDAG.getRootCoflows()){
            insertCoflow(cf);
        }
    }

    // we can implement logic that trim the coflow here.
    private void insertCoflow(Coflow cf) throws InterruptedException {
        long curTime = System.currentTimeMillis();
        cf.setStartTime(curTime);

        if (checkFlowGroups(cf , curTime)){
            onCoflowFIN(cf.getId(), curTime + Constants.COLOCATED_FG_COMPLETION_TIME);
        }
        else {
            coflowOutput.put(cf);
        }
    }

    // trim out the co-located flowgroups, and check if the whole coflow finishes, if so, return true.s
    private boolean checkFlowGroups(Coflow cf, long curTime){
        boolean cfFinished = true; // init a flag

        // need iterator because we need to remove while iterating
        Iterator<Map.Entry<String, FlowGroup>> iter = cf.getFlowGroups().entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry<String, FlowGroup> entry = iter.next();
            FlowGroup fg = entry.getValue();
            if(fg.getDstLocation() == fg.getSrcLocation()){ // job is co-located.
                fg.getAndSetFinish(curTime + Constants.COLOCATED_FG_COMPLETION_TIME); // finish right away.
                // And remove the entry
                iter.remove();
            }
            else {
                cfFinished = false; // more than one FlowGroup need to be transmitted.
            }
        }

        return cfFinished;
    }

    private void onDAGFinish(DAG dag, long timeStamp){
        System.out.println("YARN: DAG " + dag.getId() + " DONE, Took " + (dag.getFinishTime() - dag.getStartTime()) + " ms.");
    }
}
