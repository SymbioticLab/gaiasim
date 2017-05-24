package gaiasim.gaiamaster;

// The GAIA master. Runing asynchronous message processing logic.
// Three threads: 1. handling Coflow insertion (connects YARN),

import com.opencsv.CSVWriter;
import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowUpdateMessage;
import gaiasim.network.Coflow_Old;
import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.scheduler.BaselineScheduler;
import gaiasim.scheduler.PoorManScheduler;
import gaiasim.scheduler.Scheduler;
import gaiasim.spark.YARNEmulator;
import gaiasim.spark.YARNMessages;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

                    coflowPool.remove(coflowID);

                    yarnEventQueue.put(new YARNMessages(coflowID));
                    return true;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return false;
        }

        public void addCoflow(String id, Coflow cf){ // trim the co-located flowgroup before adding!
            // first add index
            for ( FlowGroup fg : cf.getFlowGroups().values()){
                flowIDtoCoflow.put( fg.getId() , cf );
            }
            //  then add coflow
            coflowPool.put(id , cf);
        }


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

//                    long curTime = System.currentTimeMillis();
//                    cf.setStartTime(curTime);

/*                    for (FlowGroup fg : cf.getFlowGroups().values()){
                        fg.setStartTime(curTime);
                        if(fg.getDstLocation() == fg.getSrcLocation()){ // job is co-located.
                            fg.getAndSetFinish(curTime); // finish right away.
                            // how to send the finish message?
                        }
                    }*/

                    // TODO: track flowgroup starttime.


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // removed runnable interface
    public class FlowUpdateSender implements Callable<Integer>{
        private List<FlowGroup_Old> fgos;
        String said;
        NetGraph ng;
        public FlowUpdateSender(String saID, List<FlowGroup_Old> fgol , NetGraph ng){
            this.fgos = fgol;
            this.said = saID;
            this.ng = ng;
        }

        @Override
        public Integer call() throws Exception {
            // first transform this into messages.
            // first group FGOs by RAs

            // This is ver 2.0 for FUM.
            FlowUpdateMessage m = new FlowUpdateMessage(fgos, ng, said);
            System.out.println("FlowUpdateSender: Created FUM: " + m.toString()); // it is working. // :-)
            sai.get(said).sendFlowUpdate_Blocking(m);

            return 1;
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

        // Should be fine, because even in old version there are two class of messages being sent,
        // we count the message number and determines the state of execution.
        LinkedBlockingQueue<PortAnnouncementMessage_Old> PAEventQueue = new LinkedBlockingQueue<PortAnnouncementMessage_Old>();


        // we have netGraph.nodes_.size() SAs
        for (String sa_id : netGraph.nodes_) {
            int id = Integer.parseInt(sa_id); // id is from 0 to n, IP from 1 to (n+1)
            sai.put(sa_id,
                    new SendingAgentInterface(sa_id, netGraph, config.getSAIP(id), config.getSAPort(id), PAEventQueue, this.ms , enablePersistentConn));
        }


        System.out.println("Master: SA Interfaces are up. ");

        // If we aren't emulating baseline, receive the port announcements
        // from SendingAgents and set appropriate flow rules.
        if (enablePersistentConn) {
            PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, PAEventQueue);
            relay.relay_ports();
        }

        System.out.println("Master: Port Announcements forwarded, starting coflowListener");

        // start the other two threads.
        coflowListener.start();
//        System.out.println("Master: starting agentController");
//        agentController.start();


        System.out.println("Master: starting periodical scheduler at every " + Constants.SCHEDULE_INTERVAL_MS + " ms.");
        // start the periodic execution of schedule(),
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

    private void schedule(){
        System.out.println("Master: Scheduling() is triggered.");

        // TODO for collocated task, finish right away (implemented during COFLOW_INSERTION)

        // take a snapshot of the current state, and move on, so as not to block other threads.
        // Ensure that every non-final field is cloned (only FlowGroup.transmitted is non-final)
        HashMap<String , Coflow_Old> outcf = new HashMap<>();
        for ( Map.Entry<String, Coflow> ecf : ms.coflowPool.entrySet()){
            Coflow_Old cfo = Coflow.toCoflow_Old_with_Trimming(ecf.getValue());
            outcf.put( cfo.getId() , cfo );
        }

        long currentTime = System.currentTimeMillis();

        try {
            HashMap<String, FlowGroup_Old> scheduled_flows = scheduler.schedule_flows(outcf, currentTime);

            // Act on the results
            sendControlMessages_Parallel(scheduled_flows);
//            sendControlMessages_Serial(scheduled_flows);

            long deltaTime = System.currentTimeMillis() - currentTime;
            System.out.println("Master: schedule() took " + deltaTime + " ms. Active CF: " + ms.coflowPool.size() + " scheduled FG: " + scheduled_flows.size());


        } catch (Exception e) { // could throw File I/O error
            e.printStackTrace();
        }

    }

    // Fully serialized version
    private void sendControlMessages_Serial(HashMap<String, FlowGroup_Old> scheduled_flows){
        Map < String , List<FlowGroup_Old>> fgobySA = scheduled_flows.values().stream()
                .collect(Collectors.groupingBy(FlowGroup_Old::getSrc_loc));

        for (Map.Entry<String , List<FlowGroup_Old>> entrybySA: fgobySA.entrySet()){

            FlowUpdateMessage m = new FlowUpdateMessage(entrybySA.getValue() , netGraph , entrybySA.getKey());
            System.out.println("FlowUpdateSender: Created FUM: " + m.toString()); // it is working. // :-)
            sai.get(entrybySA.getKey()).sendFlowUpdate_Blocking(m);

        }
    }

    // divide the messages by the corresponding SA, aggregate the RAs.
    // We don't send "UNSCHEDULE" messages, but rather let SA decide how to unsubscribe.
    private void sendControlMessages_Parallel(HashMap<String, FlowGroup_Old> scheduled_flows){
        // group FGOs by SA
        Map< String , List<FlowGroup_Old>> fgoBySA = scheduled_flows.values().stream()
                .collect(Collectors.groupingBy(FlowGroup_Old::getSrc_loc));

        // How to parallelize -> use the threadpool
        List<FlowUpdateSender> tasks= new ArrayList<>();
        for ( Map.Entry<String,List<FlowGroup_Old>> entry : fgoBySA.entrySet() ){
            tasks.add( new FlowUpdateSender(entry.getKey() , entry.getValue() , netGraph ) );
        }

        try {
            // wait for all sending to finish before proceeding
            List<Future<Integer>> futures = saControlExec.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initCSVFiles(String dagFileName , String cfFileName) throws IOException {
        String dagFilePath = outdir + dagFileName;
        String cfFilePath = outdir + cfFileName;

        CSVWriter writer = new CSVWriter(new FileWriter(dagFilePath), ',');
        String[] record = new String[4];
        record[0] = "JobID";
        record[1] = "StartTime";
        record[2] = "EndTime";
        record[3] = "JobCompletionTime";
        writer.writeNext(record);


    }

/*    public void print_statistics(String job_filename, String coflow_filename) throws java.io.IOException {
        String job_output = outdir_ + job_filename;
        CSVWriter writer = new CSVWriter(new FileWriter(job_output), ',');
        String[] record = new String[4];
        record[0] = "JobID";
        record[1] = "StartTime";
        record[2] = "EndTime";
        record[3] = "JobCompletionTime";
        writer.writeNext(record);
        for (Job j : completed_jobs_) {
            record[0] = j.getId();
            record[1] = Double.toString(j.getStart_timestamp() / Constants.MILLI_IN_SECOND_D);
            record[2] = Double.toString(j.getEnd_timestamp() / Constants.MILLI_IN_SECOND_D);
            record[3] = Double.toString((j.getEnd_timestamp() - j.getStart_timestamp()) / Constants.MILLI_IN_SECOND_D);
            writer.writeNext(record);
        }
        writer.close();

        String coflow_output = outdir_ + coflow_filename;
        CSVWriter c_writer = new CSVWriter(new FileWriter(coflow_output), ',');
        record[0] = "CoflowID";
        record[1] = "StartTime";
        record[2] = "EndTime";
        record[3] = "CoflowCompletionTime";
        c_writer.writeNext(record);

        Collections.sort(completed_coflows_, new Comparator<Coflow_Old>() {
            public int compare(Coflow_Old o1, Coflow_Old o2) {
                if (o1.getStart_timestamp() == o2.getStart_timestamp()) return 0;
                return o1.getStart_timestamp() < o2.getStart_timestamp() ? -1 : 1;
            }
        });

        for (Coflow_Old c : completed_coflows_) {
            record[0] = c.getId();
            record[1] = Double.toString(c.getStart_timestamp() / Constants.MILLI_IN_SECOND_D);
            record[2] = Double.toString(c.getEnd_timestamp() / Constants.MILLI_IN_SECOND_D);
            record[3] = Double.toString((c.getEnd_timestamp() - c.getStart_timestamp()) / Constants.MILLI_IN_SECOND_D);
            c_writer.writeNext(record);
        }
        c_writer.close();
    }*/
}
