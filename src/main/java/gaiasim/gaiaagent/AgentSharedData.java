package gaiasim.gaiaagent;

// This is the data shared between the workers, inside the Sending Agent


import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("Duplicates")

public class AgentSharedData {
    private static final Logger logger = LogManager.getLogger();

    final String saID;
    final String saName; // the name of Data Center in the trace file.

    public void pushStatusUpdate() {
        int size = flowGroups.size();
        if(size == 0){
//            System.out.println("FG_SIZE = 0");
            return;         // if there is no data to send (i.e. the master has not come online), we simply skip.
        }

//        GaiaMessageProtos.FlowStatusReport statusReport = statusReportBuilder.build();
        GaiaMessageProtos.FlowStatusReport statusReport = buildCurrentFlowStatusReport();

        try {
            worker_to_ctrlMsgQueue.put( new Worker_to_CTRLMsg(statusReport));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("finished pushing status report\n{}", statusReport);

//        while ( !isStreamReady ) {
//            initStream();
//            clientStreamObserver.onNext(statusReport);
//        }


    }

    enum SAState {
        IDLE, CONNECTING, READY
    }

    SAState saState = SAState.IDLE;

    CountDownLatch readySignal = new CountDownLatch(1);

    AtomicBoolean isSendingHeartBeat = new AtomicBoolean(false);

    CountDownLatch cnt_StartedConnections = null;

    LinkedBlockingQueue<GaiaMessageProtos.FlowUpdate> fumQueue = new LinkedBlockingQueue<>();

    LinkedBlockingQueue<Worker_to_CTRLMsg> worker_to_ctrlMsgQueue = new LinkedBlockingQueue<>();

    // moved the rpcClient to shared.
    AgentRPCClient rpcClient;

//    public HashMap<String, PConnection[]> connection_pools_ = new HashMap<String, PConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
//    public HashMap<String, PConnection> connections_ = new HashMap<String, PConnection>();

    NetGraph netGraph;

    // TODO rethink about the data structures here. the consistency between the following two?

    // TODO do we need ConcurrentHashMap?
    // fgID -> FGI. FlowGroups that are currently being sent by this SendingAgent
    public ConcurrentHashMap<String, FlowGroupInfo> flowGroups = new ConcurrentHashMap<String, FlowGroupInfo>();

    // RAID , pathID -> FGID -> subscription info // ArrayList works good here!
    public HashMap<String , ArrayList< ConcurrentHashMap<String , SubscriptionInfo> > >subscriptionRateMaps = new HashMap<>();

    // raID , pathID -> workerQueue.
    HashMap<String, LinkedBlockingQueue<CTRL_to_WorkerMsg>[]> workerQueues = new HashMap<>();

//    public List< HashMap<String , SubscriptionInfo> > subscriptionRateMaps;


    public AgentSharedData(String saID, NetGraph netGraph) {
        this.saID = saID;
        this.saName = Constants.node_id_to_trace_id.get(saID);
        this.netGraph = netGraph;

//        IMPORTANT: initializing subscriptionRateMaps
        for (String ra_id : netGraph.nodes_) {
            if (!saID.equals(ra_id)) { // don't consider path to SA itself.
                // because apap is consistent among different programs.
                int pathSize = netGraph.apap_.get(saID).get(ra_id).size();
                ArrayList<ConcurrentHashMap<String, SubscriptionInfo>> maplist = new ArrayList<>(pathSize);
                subscriptionRateMaps.put(ra_id, maplist);

                for (int i = 0; i < pathSize; i++) {
                    maplist.add(new ConcurrentHashMap<>());
                }
            }
        }

    }


    public void finishFlow(String fgID){

        // null pointer because of double sending FG_FIN
        if (flowGroups.get(fgID) == null){
            // already sent the FIN message, do nothing
            return;
        }

        if(flowGroups.get(fgID).getFlowState() == FlowGroupInfo.FlowState.FIN){
            // already sent the FIN message, do nothing
            logger.warn("Already sent the FIN for {}", fgID);
            flowGroups.remove(fgID);
            return;
        }

        flowGroups.get(fgID).setFlowState(FlowGroupInfo.FlowState.FIN);
        logger.info("Sending FLOW_FIN for {} to CTRL" , fgID);
        rpcClient.sendFG_FIN(fgID);
        flowGroups.remove(fgID);
    }


    // methods to update the flowGroups and subscriptionRateMaps
    public void startFlow(String raID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge) {
        // add this flowgroup when not existent // only accept volume from CTRL at this point.
        if( flowGroups.containsKey(fgID)){
            logger.error("START failed: an existing flow!");
            return;
        }

        FlowGroupInfo fgi = new FlowGroupInfo(fgID, fge.getRemainingVolume()).setFlowState(FlowGroupInfo.FlowState.RUNNING);
        flowGroups.put(fgID , fgi);

        addAllSubscription(raID, fgID, fge, fgi);

    }

    private void addAllSubscription(String raID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge, FlowGroupInfo flowGroupInfo) {
        for ( gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate.PathRateEntry pathToRate : fge.getPathToRateList() ){
            int pathID = pathToRate.getPathID();
            double rate = pathToRate.getRate();
            ConcurrentHashMap<String, SubscriptionInfo> infoMap = subscriptionRateMaps.get(raID).get(pathID);

            if (rate < 0.1){
                rate = 0.1;
                System.err.println("WARNING: rate of FUM too low: " + fgID);
            }

            flowGroupInfo.addWorkerInfo(raID, pathID);  // reverse look-up ArrayList

            if( infoMap.containsKey(fgID)){ // check whether this FlowGroup is in subscriptionMap.
                infoMap.get(fgID).setRate( rate );
                logger.error("DEBUG: this should not happen");
            }
            else { // create this info
                infoMap.put(fgID , new SubscriptionInfo(fgID, flowGroups.get(fgID) , rate ));
            }

        } // end loop for pathID
    }

    public void changeFlow(String raID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge) {

        if( flowGroups.containsKey(fgID)){

            FlowGroupInfo fgi = flowGroups.get(fgID);
            fgi.setFlowState(FlowGroupInfo.FlowState.RUNNING);

            removeAllSubscription(raID, fgID, fgi);
            addAllSubscription(raID, fgID, fge, fgi);

        } else {
            logger.warn("CHANGE/RESUME failed: a non-existing flow!"); // after FG finished, this can happen
            return;
        }


    }

    public void pauseFlow(String raID, String fgID, GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge) {
        // search for all subscription with this flowID, and remove them

        if( flowGroups.containsKey(fgID)){

            FlowGroupInfo fgi = flowGroups.get(fgID);
            fgi.setFlowState(FlowGroupInfo.FlowState.PAUSED);
            removeAllSubscription(raID, fgID, fgi);

        } else {
            logger.error("PAUSE failed: a non-existing flow!");
            return;
        }

    }

    private void removeAllSubscription(String raID, String fgID, FlowGroupInfo fgi) {

        for ( FlowGroupInfo.WorkerInfo wi : fgi.workerInfoList){
            try {
                subscriptionRateMaps.get(raID).get(wi.getPathID()).get(fgID).setRate(0);
                subscriptionRateMaps.get(raID).get(wi.getPathID()).remove(fgID);
            } catch (NullPointerException e){ // FIXME? sometimes happens
                e.printStackTrace();
            }
        }

        fgi.removeAllWorkerInfo();

    }

    public void printSAStatus() {

        StringBuilder strBuilder = new StringBuilder();
//        System.out.println("---------SA STATUS---------");
        strBuilder.append("---------SA STATUS---------\n");
        for (Map.Entry<String, FlowGroupInfo> fgie : flowGroups.entrySet()){
            FlowGroupInfo fgi = fgie.getValue();
            strBuilder.append(' ').append(fgi.getID()).append(' ').append(fgi.getFlowState()).append(' ').append(fgi.getVolume()-fgi.getTransmitted()).append('\n');

            for(FlowGroupInfo.WorkerInfo wi : fgi.workerInfoList){
                SubscriptionInfo tmpSI = subscriptionRateMaps.get(wi.getRaID()).get(wi.getPathID()).get(fgi.getID());
                strBuilder.append("  ").append(wi.getRaID()).append(' ').append(wi.getPathID()).append(' ').append(tmpSI.getRate()).append('\n');
            }

        }

        logger.info(strBuilder.toString());

    }

    public GaiaMessageProtos.FlowStatusReport buildCurrentFlowStatusReport() {

        GaiaMessageProtos.FlowStatusReport.Builder statusReportBuilder = GaiaMessageProtos.FlowStatusReport.newBuilder();

        for (Map.Entry<String, FlowGroupInfo> entry: flowGroups.entrySet()) {
            FlowGroupInfo fgi = entry.getValue();

            if (fgi.getFlowState() == FlowGroupInfo.FlowState.INIT ){
                logger.error("fgi in INIT state");
                continue;
            }
            if ( fgi.getFlowState() == FlowGroupInfo.FlowState.FIN ){
                continue;
            }
            if ( fgi.getFlowState() == FlowGroupInfo.FlowState.PAUSED) {
//                logger.info("");
                continue;
            }

//            if (fgi.getTransmitted() == 0){
//                logger.info("FG {} tx=0, status {}",fgi.getID(), fgi.getFlowState());
//                continue;
//            }

            GaiaMessageProtos.FlowStatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.FlowStatusReport.FlowStatus.newBuilder()
                    .setFinished(fgi.isFinished()).setId(fgi.getID()).setTransmitted(fgi.getTransmitted());

            statusReportBuilder.addStatus(fsBuilder);
        }

        return statusReportBuilder.build();
    }

/*    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

    public ConcurrentHashMap<String, FlowGroupInfo> getFlowGroups() { return flowGroups; }*/
}
