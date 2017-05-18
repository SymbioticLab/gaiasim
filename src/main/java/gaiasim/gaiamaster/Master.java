package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.comm.SendingAgentContact;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
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

    // Legacy fields
    // SendingAgentContacts indexed by sending agent id
    public HashMap<String, SendingAgentContact> sa_contacts_ = new HashMap<String, SendingAgentContact>();

    // SubModules of Master
    protected Thread yarnEmulator;
    protected Thread coflowListener;
    protected Thread agentController; // This is similar to the Manager eventloop in old version. // maybe multiple threads?
    protected final ScheduledExecutorService mainExec; // For periodic call of schedule()

    protected LinkedBlockingQueue<Coflow> coflowEventQueue;
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue;
    protected LinkedBlockingQueue<AgentMessage> agentEventQueue = new LinkedBlockingQueue<>();

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

    protected class AgentController implements Runnable{
        @Override
        public void run() {
            while (true) {
                // Block until we have a message to receive
                AgentMessage m = null;
                try {
                    m = agentEventQueue.take();
                    switch (m.getType()){
                        case FLOW_STATUS:
                            FlowStatusMessage fsm = (FlowStatusMessage) m;
                            if(fsm.getSize() == 1 && fsm.getIsFinished()[0]){ // single FLOW_FIN message
                                onFinishFlow(fsm.getId() , System.currentTimeMillis());
                            }
                            else { // FLOW_STATUS message.
                                int size = fsm.getSize();
                                for(int i = 0 ; i < size ; i++ ){
                                    // first get the current flowGroup ID
                                    // fsm.getId()[i];
                                    // check if it is finished , onFinishFlow
                                    // set the transmitted volume
                                    // ???
                                }
                            }


                            break;

                        case PORT_ANNOUNCEMENT:
                            System.out.println("Master: should not receive PA during normal operation");
                            break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        }
    }

    private void onFinishFlow(String[] id, long curTime) {
        // set the current status

        // check if all the coflow is finished

        // if so set coflow status, send COFLOW_FIN


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
        // TODO verify this can work, because the declared message type of event queue has changed!!!
        // Should be fine, because even in old version there are two class of messages being sent,
        // we count the message number and determines the state of execution.

        LinkedBlockingQueue<PortAnnouncementMessage_Old> PAEventQueue =
                new LinkedBlockingQueue<PortAnnouncementMessage_Old>();

        // TODO verfiy this, because in baseline, the SendingAgentContact will forward the received message into it.
        LinkedBlockingQueue<ScheduleMessage> devNull = new LinkedBlockingQueue<>();

        for (String sa_id : netGraph.nodes_) {
            sa_contacts_.put(sa_id,
                    new SendingAgentContact(sa_id, netGraph, "10.0.0." + (Integer.parseInt(sa_id) + 1), 23330,
                            devNull, PAEventQueue, !enablePersistentConn));
        }

        // If we aren't emulating baseline, receive the port announcements
        // from SendingAgents and set appropriate flow rules.
        if (enablePersistentConn) {
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, PAEventQueue);
            relay.relay_ports();
        }

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
