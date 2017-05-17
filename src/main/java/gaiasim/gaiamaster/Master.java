package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import gaiasim.network.NetGraph;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.YARNEmulator;
import gaiasim.spark.YARNMessages;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class Master {

    // immutable fields
    NetGraph netGraph;
    Scheduler scheduler;
    protected String outdir;

    // SubModules of Master
    protected Thread yarnEmulator;
    protected Thread coflowListener;
    protected LinkedBlockingQueue<Coflow> coflowEventQueue;
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue;

    // volatile stats of master


    protected class CoflowListener implements Runnable{
        @Override
        public void run() {
            System.out.println("Master: CoflowListener is up");
            while (true){
                try {
                    Coflow cf = coflowEventQueue.take();
                    System.out.println("Master: Received Coflow from YARN with ID= " + cf.getId());
                    // directly finish this coflow.
                    yarnEventQueue.put( new YARNMessages( cf.getId() ));

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Master(String gml_file, String trace_file,
                  String scheduler_type, String outdir) throws IOException {

        this.outdir = outdir;
        this.netGraph = new NetGraph(gml_file);

        // setting up interface with YARN.
        this.coflowEventQueue = new LinkedBlockingQueue<Coflow>();
        this.yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();
        this.yarnEmulator = new Thread(new YARNEmulator(trace_file , netGraph , yarnEventQueue , coflowEventQueue));
        this.coflowListener = new Thread( new CoflowListener() );

    }


    // the emulate() Thread is the main thread.
    public void emulate() {
        // setting up the states

        // start the other two threads.
        coflowListener.start();

        // start the timer loop of schedule()


        // Start the input
        yarnEmulator.start();

        // Enter main event loop (no such thing)
        while (true){

        }

    }

    public void simulate() {
        System.out.println("Simulation not supported");
        System.err.println("Simulation not supported in this version");
        System.exit(1);
    }
}
