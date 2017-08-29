package gaiasim.gaiamaster;


import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.PortAnnouncementRelayMessage;
import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.network.Coflow_Old;
import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import gaiasim.scheduler.CoflowScheduler;
import gaiasim.spark.YARNEmulator;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("Duplicates")

public class Master {
    private static final Logger logger = LogManager.getLogger();
    private final String outdir;
    private final Configuration config;
    private final CoflowScheduler scheduler;

    NetGraph netGraph;

    HashMap<String, MasterRPCClient> rpcClientHashMap;

    final MasterRPCServer rpcServer;

    MasterSharedData masterSharedData = new MasterSharedData();

    // SubModules of Master
    protected Thread yarnEmulator;
    protected Thread coflowListener;
//    protected Thread agentController; // This is similar to the Manager eventloop in old version. // maybe multiple threads?
    protected final ScheduledExecutorService mainExec; // For periodic call of schedule()

    protected final ExecutorService saControlExec;

    protected LinkedBlockingQueue<Coflow> coflowEventQueue;

//    protected LinkedBlockingQueue<AgentMessage> agentEventQueue = new LinkedBlockingQueue<>();


    protected class CoflowListener implements Runnable{
        @Override
        public void run() {
            System.out.println("Master: CoflowListener is up");
            while (true){
                try {
                    Coflow cf = coflowEventQueue.take();
                    String cfID = cf.getId();
                    System.out.println("Master: Received Coflow from YARN with ID = " + cfID);

                    masterSharedData.addCoflow(cfID , cf);
                    masterSharedData.flag_CF_ADD = true;

                    // TODO: track flowgroup starttime.

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Master(String gml_file, String trace_file,
                  String scheduler_type, String outdir, String configFile, double bw_factor, boolean isRunningOnList) throws IOException {

        this.outdir = outdir;
        this.netGraph = new NetGraph(gml_file , bw_factor );
        this.rpcClientHashMap = new HashMap<>();

        printPaths(netGraph);
        if(configFile == null){
            this.config = new Configuration(netGraph.nodes_.size(), netGraph.nodes_.size());
        }
        else {
            this.config = new Configuration(netGraph.nodes_.size(), netGraph.nodes_.size(), configFile);
        }

        this.rpcServer = new MasterRPCServer(this.config, this.masterSharedData);

//        this.masterSharedData.coflowPool = new ConcurrentHashMap<>();
        this.mainExec = Executors.newScheduledThreadPool(1);

        // setting up interface with YARN.
        this.coflowEventQueue = new LinkedBlockingQueue<Coflow>();
        this.yarnEmulator = new Thread(new YARNEmulator(trace_file , netGraph , masterSharedData.yarnEventQueue , coflowEventQueue, outdir, isRunningOnList));
        this.coflowListener = new Thread( new CoflowListener() );

        // setting up the scheduler
        scheduler = new CoflowScheduler(netGraph);

        if (scheduler_type.equals("baseline")) { // no baseline!!!
            System.err.println("No baseline");
            System.exit(1);
//            scheduler = new BaselineScheduler(netGraph);
//            enablePersistentConn = false;
        }
        else if (scheduler_type.equals("recursive-remain-flow")) {
//            scheduler = new PoorManScheduler(netGraph);
//            scheduler = new CoflowScheduler(netGraph);
//            enablePersistentConn = true;
            System.out.println("Using coflow scheduler");
        }
        else {
            System.out.println("Unrecognized scheduler type: " + scheduler_type);
            System.out.println("Scheduler must be one of { baseline, recursive-remain-flow }");
            System.exit(1);
        }

        saControlExec = Executors.newFixedThreadPool(netGraph.nodes_.size());
    }


    public void emulate() {
        LinkedBlockingQueue<PortAnnouncementMessage_Old> PAEventQueue = new LinkedBlockingQueue<PortAnnouncementMessage_Old>();

        logger.info("Starting master RPC server");

        // first start the server to receive status update
        MasterRPCServer rpcServer = new MasterRPCServer(config, this.masterSharedData);
        try {
            rpcServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }


        logger.info("Starting master RPC Client");

        // we have netGraph.nodes_.size() SAs
        for (String sa_id : netGraph.nodes_) {
            int id = Integer.parseInt(sa_id); // id is from 0 to n, IP from 1 to (n+1)
            MasterRPCClient client = new MasterRPCClient(config.getSAIP(id), config.getSAPort(id));
            rpcClientHashMap.put( sa_id , client);
            Iterator<GaiaMessageProtos.PAMessage> it = client.preparePConn();
            while (it.hasNext()) {
                GaiaMessageProtos.PAMessage pam = it.next();
//                System.out.print(pam);
                try {
                    PAEventQueue.put(new PortAnnouncementMessage_Old(pam.getSaId(), pam.getRaId(), pam.getPathId(), pam.getPortNo()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

//            sai.put(sa_id, new SendingAgentInterface(sa_id, netGraph, config.getSAIP(id), config.getSAPort(id), PAEventQueue, this.ms , enablePersistentConn));
        }

        logger.info("Connection established between SA-RA");

        // receive the port announcements from SendingAgents and set appropriate flow rules.
        PortAnnouncementRelayMessage relay = new PortAnnouncementRelayMessage(netGraph, PAEventQueue);
        relay.relay_ports();

        logger.info("All flow rules set up");

        try {
            Thread.sleep(10000); // sleep 10s for the rules to propagate // TODO test the rules first before proceeding
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start sending heartbeat
        for (String sa_id : netGraph.nodes_) {
            rpcClientHashMap.get(sa_id).startHeartBeat();
        }

        // start the other two threads.
        coflowListener.start();

        System.out.println("Master: starting periodical scheduler at every " + Constants.SCHEDULE_INTERVAL_MS + " ms.");
        // start the periodic execution of schedule()

//        final Runnable runSchedule = () -> schedule();
        final Runnable runSchedule = () -> schedule();
        ScheduledFuture<?> mainHandler = mainExec.scheduleAtFixedRate(runSchedule, 0, Constants.SCHEDULE_INTERVAL_MS, MILLISECONDS);


        // Start the input
        yarnEmulator.start();

        logger.info("Master init finished, block the main thread");
        try {
            rpcServer.blockUntilShutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void simulate() {
        System.out.println("Simulation not supported");
        System.err.println("Simulation not supported in this version");
        System.exit(1);
    }

    // print the pathway with their ID
    private void printPaths(NetGraph ng){
        for (String src : ng.nodes_){
            if (ng.apap_.get(src) == null){
                continue;
            }
            for (String dst : ng.nodes_){
                ArrayList<Pathway> list = ng.apap_.get(src).get(dst);
                if(list != null && list.size() > 0 ){
                    System.out.println("Paths for " + src + " - " + dst + " :");
                    for (int i = 0 ; i < list.size() ; i++){
                        System.out.println( i + " : " + list.get(i));
                    }
                }
            }
        }
    }

    // the new version of schedule()
    // 1. check in the last interval if anything happens, and determine a fast schedule or re-do the sorting process
    private void schedule(){
//        logger.info("schedule_New(): CF_ADD: {} CF_FIN: {} FG_FIN: {}", masterSharedData.flag_CF_ADD, masterSharedData.flag_CF_FIN, masterSharedData.flag_FG_FIN);

        long currentTime = System.currentTimeMillis();
        List<FlowGroup_Old> scheduledFGs = new ArrayList<>(0);
        List<FlowGroup_Old> FGsToSend = new ArrayList<>();

        // snapshoting and converting
        HashMap<String , Coflow_Old> outcf = new HashMap<>();
        for ( Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()){
            Coflow_Old cfo = Coflow.toCoflow_Old_with_Trimming(ecf.getValue());
            outcf.put( cfo.getId() , cfo );
        }

//        printCFList(outcf);

        if (masterSharedData.flag_CF_ADD){ // redo sorting, may result in preemption
            masterSharedData.flag_CF_ADD = false;
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;

            // TODO update the CF_Status in scheduler
            scheduler.resetCFList(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGs = scheduler.scheduleRRF(currentTime);

                FGsToSend = parseFlowState(masterSharedData, scheduledFGs);
                sendControlMessages_Async(FGsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else if (masterSharedData.flag_CF_FIN){ // no LP-sort, just update volume status and re-schedule
            masterSharedData.flag_CF_FIN = false;
            masterSharedData.flag_FG_FIN = false;
            scheduler.handleCoflowFIN(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGs = scheduler.scheduleRRF(currentTime);

                FGsToSend = parseFlowState(masterSharedData, scheduledFGs);
                sendControlMessages_Async(FGsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else if (masterSharedData.flag_FG_FIN){ // no-reschedule, just pick up a new flowgroup.
            masterSharedData.flag_FG_FIN = false;
            scheduler.handleFlowGroupFIN(outcf);

//            scheduler.printCFList();

            try {
                scheduledFGs = scheduler.scheduleRRF(currentTime);

                FGsToSend = parseFlowState(masterSharedData, scheduledFGs);
                sendControlMessages_Async(FGsToSend);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {  // if none, NOP
            return; // no need to print out the execution time.
        }

        long deltaTime = System.currentTimeMillis() - currentTime;

        StringBuilder fgoContent = new StringBuilder("\n");
        for ( FlowGroup_Old fgo : FGsToSend){
            fgoContent.append(fgo.getId()).append(' ').append(fgo.paths).append(' ').append(fgo.getFlowState()).append('\n');
        }
        logger.info("schedule(): took {} ms. Active CF: {} Scheduled FG: {} FG content:{}", deltaTime , masterSharedData.coflowPool.size(), scheduledFGs.size(), fgoContent);

//        printMasterState();
    }

    // update the flowState in the CFPool, before sending out the information.
    private List<FlowGroup_Old> parseFlowState(MasterSharedData masterSharedData, List<FlowGroup_Old> scheduledFGs) {
        List<FlowGroup_Old> fgoToSend = new ArrayList<>();
        HashMap<String, FlowGroup_Old> fgoHashMap = new HashMap<>();

        // first convert List to hashMap
        for (FlowGroup_Old fgo : scheduledFGs){
            fgoHashMap.put(fgo.getId(), fgo);
        }

        // traverse all FGs in CFPool
        for ( Map.Entry<String, Coflow> ecf : masterSharedData.coflowPool.entrySet()) { // snapshoting should not be a problem
            Coflow cf = ecf.getValue();
            for ( Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()){
                FlowGroup fg = fge.getValue();
                if (fg.getFlowState() == FlowGroup.FlowState.FIN){
                    continue; // ignore finished, they shall be removed soon
                }
                else if (fg.getFlowState() == FlowGroup.FlowState.RUNNING) { // may pause/change the running flow
                    if (fgoHashMap.containsKey(fg.getId())){ // we may need to change, if the path/rate are different TODO: speculatively send change message
                        fgoToSend.add (fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old.FlowState.CHANGING) ); // running flow needs to change
                    }
                    else { // we need to pause
                        fg.setFlowState(FlowGroup.FlowState.PAUSED);
                        fgoToSend.add ( FlowGroup.toFlowGroup_Old(fg, 0).setFlowState(FlowGroup_Old.FlowState.PAUSING) );
                    }
                }
                else { // case: NEW/PAUSED
                    if (fgoHashMap.containsKey(fg.getId())) { // we take action only if the flow get (re)scheduled
                        if (fg.getFlowState() == FlowGroup.FlowState.NEW){ // start the flow
                            fg.setFlowState(FlowGroup.FlowState.RUNNING);
                            fgoToSend.add (fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old.FlowState.STARTING) );
                            masterSharedData.flowStartCnt ++ ;
                        }
                        else if (fg.getFlowState() == FlowGroup.FlowState.PAUSED){ // RESUME the flow
                            fg.setFlowState(FlowGroup.FlowState.RUNNING);
                            fgoToSend.add (fgoHashMap.get(fg.getId()).setFlowState(FlowGroup_Old.FlowState.CHANGING) );
                        }

                    }
                }
            }

        }

        return fgoToSend;
    }

    private void sendControlMessages_Async(List<FlowGroup_Old> scheduledFGs) {
        // group FGOs by SA
        Map< String , List<FlowGroup_Old>> fgoBySA = scheduledFGs.stream()
                .collect(Collectors.groupingBy(FlowGroup_Old::getSrc_loc));

        // just post the update to RPCClient, not waiting for reply
        for ( Map.Entry<String,List<FlowGroup_Old>> entry : fgoBySA.entrySet() ){
            String saID = entry.getKey();
            List<FlowGroup_Old> fgforSA = entry.getValue();

            // call async RPC
            rpcClientHashMap.get(saID).setFlow(fgforSA, netGraph , saID);
        }

/*        // How to parallelize -> use the threadpool
        List<FlowUpdateSender> tasks= new ArrayList<>();
        for ( Map.Entry<String,List<FlowGroup_Old>> entry : fgoBySA.entrySet() ){
            tasks.add( new FlowUpdateSender(entry.getKey() , entry.getValue() , netGraph ) );
        }

        try {
            // wait for all sending to finish before proceeding
            List<Future<Integer>> futures = saControlExec.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }

    public void printMasterState(){
        StringBuilder str = new StringBuilder("-----Master state-----\n");
        int paused = 0;
        int running = 0;
        for( Map.Entry<String, Coflow> cfe : masterSharedData.coflowPool.entrySet()){
            Coflow cf = cfe.getValue();

            str.append(cf.getId()).append('\n');

            for ( Map.Entry<String, FlowGroup> fge : cf.getFlowGroups().entrySet()){
                FlowGroup fg = fge.getValue();
                str.append(' ').append(fge.getKey()).append(' ').append(fg.getFlowState())
                        .append(' ').append(fg.getTransmitted()).append(' ').append(fg.getTotalVolume()).append('\n');
                if (fg.getFlowState() == FlowGroup.FlowState.PAUSED) {
                    paused++;
                } else if (fg.getFlowState() == FlowGroup.FlowState.RUNNING){
                    running++;
                }
            }
        }

        str.append("stats s: ").append(masterSharedData.flowStartCnt).append(" f: ").append(masterSharedData.flowFINCnt)
            .append(" p: ").append(paused).append(" r: ").append(running).append('\n');
        logger.info(str);
    }


/*    void testSA (String saIP, int saPort){

        final ManagedChannel channel = ManagedChannelBuilder.forAddress(saIP, saPort).usePlaintext(true).build();
        SAServiceGrpc.SAServiceBlockingStub blockingStub = SAServiceGrpc.newBlockingStub(channel);
        GaiaMessageProtos.PAM_REQ req = GaiaMessageProtos.PAM_REQ.newBuilder().build();

        Iterator<GaiaMessageProtos.PAMessage> it = blockingStub.prepareConnections(req);
        while (it.hasNext()) {
            System.out.print(it.next());
        }
        channel.shutdown();
    }*/
}
