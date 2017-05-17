package gaiasim.spark;

// This emulates a YARN, it takes in trace.txt, and outputs Coflows.
// A thread DAGReader reads the trace.txt and insert DAG at defined arrival time.
// A event loop processes the DAG_ARRIVAL and COFLOW_FIN.
// It maintains the state of active coflows and send coflows:
//  (1) insert initial Coflows w/o dependencies at the trace-specified time
//  (2) dependent coflows when their dependencies have already been met (i.e. on COFLOW_FIN)

import gaiasim.gaiamaster.Coflow;
import gaiasim.network.NetGraph;

import java.util.concurrent.LinkedBlockingQueue;

public class YARNEmulator implements Runnable {

    String tracefile;
    NetGraph netGraph;
    LinkedBlockingQueue<YARNMessages> yarnEventQueue;
    LinkedBlockingQueue<Coflow> coflowOutput;
    Thread dagThread;


    @Override
    public void run() {

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


    public YARNEmulator(String tracefile, NetGraph netGraph , LinkedBlockingQueue<Coflow> coflowOutput) {
        this.tracefile = tracefile;
        this.netGraph = netGraph;
        this.coflowOutput = coflowOutput;

        yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();

        // init the YARN, read the trace and prepare a list of DAGs.
        dagThread = new Thread(new DAGReader(tracefile , netGraph , yarnEventQueue));


//        DAGReader dagReader = new DAGReader(tracefile , netGraph , yarnEventQueue);

        //dagReader


    }

    private void onCoflowFIN(String fin_coflow_id) {
        // get the DAG from dag_pool , by id.

        // dag.onFIN(id)
//        DAG dag = new DAG();

        // get newCoflows from DAG. and schedule them.
//        dag.onCoflowFIN(fin_coflow_id);

    }

    private void onDAGArrival(DAG arrivedDAG) {
        // first add dag to the pool.

        // then insert the root coflows to GAIA (into coflowOutput)
//        coflowOutput.put();
    }
}
