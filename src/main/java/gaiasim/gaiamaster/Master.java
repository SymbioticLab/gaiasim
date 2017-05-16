package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import gaiasim.network.NetGraph;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.YARNEmulator;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class Master {

    NetGraph netGraph;
    Scheduler scheduler;

    private String outdir;



    private YARNEmulator yarn;
    private LinkedBlockingQueue<Coflow> coflowInput;

    public Master(String gml_file, String trace_file,
                  String scheduler_type, String outdir) throws IOException {

        this.outdir = outdir;
        this.netGraph = new NetGraph(gml_file);

        coflowInput = new LinkedBlockingQueue<Coflow>();

        yarn = new YARNEmulator( trace_file , netGraph , coflowInput);



    }


}