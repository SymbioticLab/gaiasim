package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.comm.ScheduleMessage;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.gaiamessage.FlowUpdateMessage;
import gaiasim.network.Coflow_Old;
import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.scheduler.BaselineScheduler;
import gaiasim.scheduler.PoorManScheduler;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.YARNEmulator;
import gaiasim.spark.YARNMessages;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Master {

    // immutable fields
    NetGraph netGraph;
    Scheduler scheduler;
    protected String outdir;
    protected boolean enablePersistentConn;
    protected Configuration config;

    // Legacy fields
    // SendingAgentContacts indexed by sending agent id
    public HashMap<String, SendingAgentInterface> sai = new HashMap<String, SendingAgentInterface>();

    // SubModules of Master
    protected Thread yarnEmulator;
    protected Thread coflowListener;
    protected Thread agentController; // This is similar to the Manager eventloop in old version. // maybe multiple threads?
    protected final ScheduledExecutorService mainExec; // For periodic call of schedule()

    protected final ExecutorService saControlExec;

    protected LinkedBlockingQueue<Coflow> coflowEventQueue;
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue;
    protected LinkedBlockingQueue<AgentMessage> agentEventQueue = new LinkedBlockingQueue<>();

    // volatile states of master
    public class MasterState{

        public volatile ConcurrentHashMap<String , Coflow> coflowPool;

        // index for searching flowGroup in this data structure.
        // only need to add entry, no need to delete entry. TODO verify this.
        public volatile ConcurrentHashMap<String , Coflow> flowIDtoCoflow;

        // handles coflow finish.
        public synchronized boolean onFinishCoflow(String coflowID) {
            System.out.println("Master: trying to finish Coflow: " + coflowID);

            try {

                // use the get and set method, to make sure that:
                // 1. the value is false before we send COFLOW_FIN
                // 2. the value must be set to true, after whatever we do.
                if(  !coflowPool.get(coflowID).getAndSetFinished(true) ){
                    yarnEventQueue.put(new YARNMessages(coflowID));
                    return true;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return false;
        }

        public void addCoflow(String id, Coflow cf){
            // first add index
            for ( FlowGroup fg : cf.getFlowGroups().values()){
                flowIDtoCoflow.put( fg.getId() , cf );
            }
            //  then add coflow
            coflowPool.put(id , cf);
        }

//        public Coflow getCoflowFromFlowGroup(String id) {
//            return flowIDtoCoflow.get(id);
//        }

        public FlowGroup getFlowGroup(String id){
            if( flowIDtoCoflow.containsKey(id)){
                return flowIDtoCoflow.get(id).getFlowGroup(id);
            }
            else {
                return null;
            }
        }

        // TODO: set the concurrency level.
        public MasterState(){
            this.coflowPool = new ConcurrentHashMap<>();
            this.flowIDtoCoflow = new ConcurrentHashMap<>();
        }
    }

    MasterState ms = new MasterState();


    protected class CoflowListener implements Runnable{
        @Override
        public void run() {
            System.out.println("Master: CoflowListener is up");
            while (true){
                try {
                    Coflow cf = coflowEventQueue.take();
                    String cfID = cf.getId();
                    System.out.println("Master: Received Coflow from YARN with ID = " + cfID);

                    ms.addCoflow(cfID , cf);

                    // TODO: track flowgroup starttime.

//                    ms.coflowPool.put(cfID , cf);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class FlowUpdateSender implements Runnable{
        private List<FlowGroup_Old> fgos;
        String said;
        NetGraph ng;
        public FlowUpdateSender(String saID, List<FlowGroup_Old> fgol , NetGraph ng){
            this.fgos = fgol;
            this.said = saID;
            this.ng = ng;
        }

        @Override
        public void run() {
            // first transform this into messages.
            // create a multimap for different RAs, (RA -> FGO).
            ArrayListMultimap<String , FlowGroup_Old> raToFGO = ArrayListMultimap.create();

            for ( FlowGroup_Old f : fgos){
                raToFGO.put( f.getDst_loc() , f);
            }

            // TODO Can we parallelize this? hard.
            for (String raID : raToFGO.keySet()){
                // for each RA, we generate a FUM: fgID[] , fgVol[] , rates [fg][path]
                int sizeOfFGO = raToFGO.get(raID).size();
                assert (sizeOfFGO == 0);
                int sizeOfPaths = raToFGO.get(raID).get(0).paths.size(); // should be consistent among FGOs TODO verify
                String [] fgID = new String[sizeOfFGO];
                double [] fgVol = new double[sizeOfFGO];
                double[][] rates = new double[sizeOfFGO][sizeOfPaths];

                for (int i = 0 ; i < sizeOfFGO ; i++){
                    FlowGroup_Old fgo = raToFGO.get(raID).get(i);
                    fgID [i] = fgo.getId();
                    fgVol[i] = fgo.remaining_volume();

                    for (int j = 0; j < sizeOfPaths ; j++){

                        Pathway p = fgo.paths.get(j);
                        int pid = ng.get_path_id(p);

                        // TODO verify this. verify that pid maps to [0,MAX_P]
                        rates[i][pid] = p.getBandwidth();

                    }
                }

                FlowUpdateMessage m = new FlowUpdateMessage(raID , sizeOfFGO , sizeOfPaths , fgID, fgVol , rates);
                sai.get(said).sendFlowUpdate_Blocking(m);
            }
        }
    }

    public Master(String gml_file, String trace_file,
                  String scheduler_type, String outdir, String config) throws IOException {

        this.outdir = outdir;
        this.netGraph = new NetGraph(gml_file);
        if(config == null){
            this.config = new Configuration(netGraph.nodes_.size(), netGraph.nodes_.size());
        }
        else {
            this.config = new Configuration(netGraph.nodes_.size(), netGraph.nodes_.size(), config);
        }


        this.ms.coflowPool = new ConcurrentHashMap<>();
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

        saControlExec = Executors.newFixedThreadPool(netGraph.nodes_.size());
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

/*        for (String sa_id : netGraph.nodes_) {
            sai.put(sa_id,
                    new SendingAgentContact(sa_id, netGraph, "10.0.0." + (Integer.parseInt(sa_id) + 1), 23330,
                            devNull, PAEventQueue, !enablePersistentConn));
        }*/

        // we have netGraph.nodes_.size() SAs
        for (String sa_id : netGraph.nodes_) {
            int id = Integer.parseInt(sa_id); // id is from 0 to n, IP from 1 to (n+1)
            sai.put(sa_id,
                    new SendingAgentInterface(sa_id, netGraph, config.getSAIP(id), config.getSAPort(id), PAEventQueue, this.ms , enablePersistentConn));
        }


        System.out.println("Master: SA Interfaces are up.");

        // If we aren't emulating baseline, receive the port announcements
        // from SendingAgents and set appropriate flow rules.
        if (enablePersistentConn) {
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, PAEventQueue);
            relay.relay_ports();
        }

        System.out.println("Port Announcements forwarded, starting coflowListener");

        // start the other two threads.
        coflowListener.start();
//        System.out.println("Master: starting agentController");
//        agentController.start();


        System.out.println("Master: starting periodical scheduler at every " + Constants.SCHEDULE_INTERVAL_MS + " ms.");
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

        // TODO for collocated task, finish right away!

        // take a snapshot of the current state, and moves on, so as not to block other threads.
        HashMap<String , Coflow_Old> outcf = new HashMap<>();
        for ( Map.Entry<String, Coflow> ecf : ms.coflowPool.entrySet()){
            Coflow_Old cfo = Coflow.toCoflow_Old(ecf.getValue());
            outcf.put( cfo.getId() , cfo );
        }

        long currentTime = System.currentTimeMillis();

        try {
            HashMap<String, FlowGroup_Old> scheduled_flows = scheduler.schedule_flows(outcf, currentTime);
            // Act on the results // TODO send the flow_messages.
            sendControlMessages(scheduled_flows);



        } catch (Exception e) { // could throw File I/O error
            e.printStackTrace();
        }

    }

    // divide the messages by the corresponding SA, aggregate by RAs.
    // We don't send "UNSCHEDULE" messages, but rather let SA decide how to unsubscribe.
    public void sendControlMessages(HashMap<String, FlowGroup_Old> scheduled_flows){

        // transform this into a multimap, manually , maps f.getSrc_loc() (i.e. saID) -> f
        ArrayListMultimap<String , FlowGroup_Old> updates = ArrayListMultimap.create();

        for (FlowGroup_Old fgo : scheduled_flows.values()){
            // we sort the flowGroup according the src_location (i.e. sendingAgentInterface, and pass on the job.)
            updates.put( fgo.getSrc_loc()  , fgo  );
        }

        // How to parallelize? use the threadpool
        for ( String saID : updates.keySet() ){
            saControlExec.submit( new FlowUpdateSender(saID , updates.get(saID) , netGraph ) );
        }

/*        for (FlowGroup_Old f : preempted_flowGroups) {
            if (!active_flows_.containsKey(f.getId())) {
                active_flows_.put(f.getId(), f);
            }
        }

        // Send FLOW_UPDATEs and FLOW_STARTs based on new schedule
        // TODO: Consider parallelizing this so that messages intended
        //       for different SAs don't block on each other.
        for (String flow_id : active_flows_.keySet()) {
            FlowGroup_Old f = active_flows_.get(flow_id);

            if (!f.isStarted_sending()) {
                sa_contacts_.get(f.getSrc_loc()).start_flow(f);

                // Only update started_sending if we're running baseline
                f.setStarted_sending(is_baseline_);
            }
        }*/

    }


}
