package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.comm.SendingAgentContact;
import gaiasim.network.Coflow_Old;
import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.BaselineScheduler;
import gaiasim.scheduler.PoorManScheduler;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.YARNEmulator;
import gaiasim.spark.YARNMessages;
import gaiasim.util.Constants;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Master {

    // immutable fields
    NetGraph netGraph;
    Scheduler scheduler;
    protected String outdir;
    protected boolean enablePersistentConn;

    // SubModules of Master
    protected Thread yarnEmulator;
    protected Thread coflowListener;
    protected Thread agentController; // maybe multiple threads?
    protected final ScheduledExecutorService mainExec; // For periodic call of schedule()

    protected LinkedBlockingQueue<Coflow> coflowEventQueue;
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue;

    // volatile states of master
    protected volatile ConcurrentHashMap<String , Coflow> coflowPool;


    protected class CoflowListener implements Runnable{
        @Override
        public void run() {
            System.out.println("Master: CoflowListener is up");
            while (true){
                try {
                    Coflow cf = coflowEventQueue.take();
                    String cfID = cf.getId();
                    System.out.println("Master: Received Coflow from YARN with ID = " + cfID);

                    coflowPool.put(cfID , cf);

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
        this.coflowPool = new ConcurrentHashMap<>();
        this.mainExec = Executors.newScheduledThreadPool(1);

        // setting up interface with YARN.
        this.coflowEventQueue = new LinkedBlockingQueue<Coflow>();
        this.yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();
        this.yarnEmulator = new Thread(new YARNEmulator(trace_file , netGraph , yarnEventQueue , coflowEventQueue));
        this.coflowListener = new Thread( new CoflowListener() );

        // setting up the scheduler
        if (scheduler_type.equals("baseline")) {
            scheduler = new BaselineScheduler(netGraph);
            enablePersistentConn = false;
        }
        else if (scheduler_type.equals("recursive-remain-flow")) {
            scheduler = new PoorManScheduler(netGraph);
            enablePersistentConn = true;
        }
        else {
            System.out.println("Unrecognized scheduler type: " + scheduler_type);
            System.out.println("Scheduler must be one of { baseline, recursive-remain-flow }");
            System.exit(1);
        }

    }


    // the emulate() Thread is the main thread.
    public void emulate() {
        // setting up the states

        // Set up our SendingAgentContacts
/*        for (String sa_id : netGraph.nodes_) {
            sa_contacts_.put(sa_id,
                    new SendingAgentContact(sa_id, netGraph, "10.0.0." + (Integer.parseInt(sa_id) + 1), 23330,
                            message_queue_, port_announcements, is_baseline_));
        }

        // If we aren't emulating baseline, receive the port announcements
        // from SendingAgents and set appropriate flow rules.
        if (!isBaseline) {
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, port_announcements);
            relay.relay_ports();
        }*/

        // start the other two threads.
        coflowListener.start();


        // start the periodic execution of schedule()
        final Runnable runSchedule = () -> schedule();
        ScheduledFuture<?> mainHandler = mainExec.scheduleAtFixedRate(runSchedule, 0, Constants.SCHEDULE_INTERVAL_MS, MILLISECONDS);


        // Start the input
        yarnEmulator.start();



/*        // Enter main event loop (no such thing)
        while (true){

        }*/

    }

    public void simulate() {
        System.out.println("Simulation not supported");
        System.err.println("Simulation not supported in this version");
        System.exit(1);
    }

    public void schedule(){
        System.out.println("Master: Scheduling......");

        // take a snapshot of the current state, and moves on, so as not to block other threads.
        HashMap<String , Coflow_Old> outcf = new HashMap<>();
        for ( Map.Entry<String, Coflow> ecf : coflowPool.entrySet()){
            Coflow_Old cfo = Coflow.toCoflow_Old(ecf.getValue());
            outcf.put( cfo.getId() , cfo );
        }

        long currentTime = System.currentTimeMillis();

        try {
            HashMap<String, FlowGroup_Old> scheduled_flows = scheduler.schedule_flows(outcf, currentTime);
            // Act on the results



        } catch (Exception e) { // could throw File I/O error
            e.printStackTrace();
        }

    }

    // handles coflow finish.
    private void onFinishCoflow(String coflowID) {

    }
}
