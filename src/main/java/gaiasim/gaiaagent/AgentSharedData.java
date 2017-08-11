package gaiasim.gaiaagent;

// This is the data shared between the workers, inside the Sending Agent


import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("Duplicates")

public class AgentSharedData {
    final String saID;
    final String saName; // the name of Data Center in the trace file.

    enum SAState {
        IDLE, CONNECTING, READY
    }

    SAState saState = SAState.IDLE;

    CountDownLatch readySignal = new CountDownLatch(1);

    LinkedBlockingQueue<GaiaMessageProtos.FlowUpdate> fumQueue = new LinkedBlockingQueue<>();

    // moved the rpcClient to shared.
    AgentRPCClient rpcClient;

//    public HashMap<String, PConnection[]> connection_pools_ = new HashMap<String, PConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
//    public HashMap<String, PConnection> connections_ = new HashMap<String, PConnection>();

    NetGraph netGraph;

    // TODO rethink about the data structures here. the consistency between the following two?

    // TODO do we need ConcurrentHashMap?
    // FlowGroups that are currently being sent by this SendingAgent
    public ConcurrentHashMap<String, FlowGroupInfo> flowGroups = new ConcurrentHashMap<String, FlowGroupInfo>();

    // RAID , pathID -> FGID -> subscription info // ArrayList works good here!
    public ConcurrentHashMap<String , ArrayList< HashMap<String , SubscriptionInfo> > >subscriptionRateMaps = new ConcurrentHashMap<>();

    // raID , pathID -> workerQueue.
    HashMap<String, LinkedBlockingQueue<SubscriptionMessage>[]> workerQueues = new HashMap<>();

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
                ArrayList<HashMap<String, SubscriptionInfo>> maplist = new ArrayList<>(pathSize);
                subscriptionRateMaps.put(ra_id, maplist);

                for (int i = 0; i < pathSize; i++) {
                    maplist.add(new HashMap<>());
                }
            }
        }

    }


    public void finishFlowGroup(String fgID){

        // TODO send FIN message to CTRL immediately
        rpcClient.sendFG_FIN(fgID);

        flowGroups.remove(fgID);
    }


/*    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

    public ConcurrentHashMap<String, FlowGroupInfo> getFlowGroups() { return flowGroups; }*/
}
