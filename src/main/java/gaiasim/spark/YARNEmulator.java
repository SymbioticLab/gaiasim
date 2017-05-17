package gaiasim.spark;

// This emulates a YARN, it takes in trace.txt, and outputs Coflows.
// A thread DAGReader reads the trace.txt and insert DAG at defined arrival time.
// A event loop processes the DAG_ARRIVAL and COFLOW_FIN.
// It maintains the state of active coflows and send coflows:
//  (1) insert initial Coflows w/o dependencies at the trace-specified time
//  (2) dependent coflows when their dependencies have already been met (i.e. on COFLOW_FIN)

import gaiasim.gaiamaster.Coflow;
import gaiasim.network.NetGraph;

import java.util.HashMap;
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
                        onCoflowFIN(m.FIN_coflow_ID);

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

//        yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();

        // init the YARN, read the trace and prepare a list of DAGs.
        dagThread = new Thread(new DAGReader(tracefile , netGraph , yarnEventQueue));


//        DAGReader dagReader = new DAGReader(tracefile , netGraph , yarnEventQueue);

        //dagReader


    }

    // TODO Handle finish of a coflow.
    private void onCoflowFIN(String fin_coflow_id) throws InterruptedException {
        System.out.println("YARN: Received FIN for Coflow " + fin_coflow_id);
        // get the owning DAG from dag_pool , by Coflow_id.
        String [] split = fin_coflow_id.split(":"); // DAG_ID = split[0]
        if (dagPool.containsKey(split[0])){
            DAG dag = dagPool.get(split[0]);
            // get new coflows and schedule them
            for( Coflow cf : dag.onCoflowFIN(fin_coflow_id)){
                coflowOutput.put(cf);
            }
            // Check if DAG is done
            if (dag.isDone()){
                System.out.println("YARN: DAG " + dag.getId() + " DONE, Took " + (dag.getFinishTime() - dag.getArrivalTime()) + " ms.");
            }
        }
        else {
            System.err.println("YARN: Received FIN for Coflow that is not in DAG Pool.");
        }
    }

    // TODO Handle submission of DAGs.
    private void onDAGArrival(DAG arrivedDAG) throws InterruptedException {
        System.out.println("YARN: DAG " + arrivedDAG.getId() + " arrived at " + arrivedDAG.getArrivalTime() + " s.");
        arrivedDAG.onStart();
        // first add dag to the pool.
        dagPool.put(arrivedDAG.getId() , arrivedDAG);

        // then insert the root coflows to GAIA (into coflowOutput)
        for ( Coflow cf : arrivedDAG.getRootCoflows()){
            coflowOutput.put(cf);
        }
    }
}
