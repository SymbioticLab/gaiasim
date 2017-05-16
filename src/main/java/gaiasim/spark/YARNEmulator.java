package gaiasim.spark;

// This emulates a YARN, it takes in trace.txt, and outputs Coflows.

import gaiasim.gaiamaster.Coflow;
import gaiasim.network.NetGraph;

import java.util.concurrent.LinkedBlockingQueue;

public class YARNEmulator implements Runnable {

    String tracefile;

    NetGraph netGraph;

    LinkedBlockingQueue<YARNMessages> yarnEventQueue;
    LinkedBlockingQueue<Coflow> coflowOutput;
    Thread dagThread;

    public YARNEmulator(String tracefile, NetGraph netGraph , LinkedBlockingQueue<Coflow> coflowOutput) {
        this.tracefile = tracefile;
        this.netGraph = netGraph;
        this.coflowOutput = coflowOutput;

        yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();

        // init the YARN, read the trace and prepare a list of DAGs.
        dagThread = new Thread(new DAGReader(tracefile , netGraph , yarnEventQueue));


        //


//        DAGReader dagReader = new DAGReader(tracefile , netGraph , yarnEventQueue);

        //dagReader



    }




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

    private void onCoflowFIN(String fin_coflow_id) {
        // get the DAG from dag_pool , by id.

        // dag.onFIN(id)

    }

    private void onDAGArrival(DAG arrivedDAG) {
        // first add dag to the pool.

        // then insert the root coflows to GAIA (into coflowOutput)
//        coflowOutput.put();
    }
}
