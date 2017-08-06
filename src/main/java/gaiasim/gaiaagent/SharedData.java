package gaiasim.gaiaagent;

// This is the data shared between the workers, inside the Sending Agent


import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("Duplicates")

public class SharedData {
    final String saID;
    final String saName; // the previous traceID.

//    public HashMap<String, PConnection[]> connection_pools_ = new HashMap<String, PConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
//    public HashMap<String, PConnection> connections_ = new HashMap<String, PConnection>();

    NetGraph netGraph;

    // TODO rethink about the data structures here. the consistency between the following two?
    // FlowGroups that are currently being sent by this SendingAgent
    public HashMap<String, FlowGroupInfo> flowGroups = new HashMap<String, FlowGroupInfo>();

    // RAID , pathID -> FGID -> subscription info // ArrayList works good here!
    public HashMap<String , ArrayList< HashMap<String , SubscriptionInfo> > >subscriptionRateMaps = new HashMap<>();

    // raID , pathID -> workerQueue.
    HashMap<String, LinkedBlockingQueue<SubscriptionMessage>[]> workerQueues = new HashMap<>();

//    public List< HashMap<String , SubscriptionInfo> > subscriptionRateMaps;


    public SharedData(String saID, NetGraph netGraph) {
        this.saID = saID;
        this.saName = Constants.node_id_to_trace_id.get(saID);
        this.netGraph = netGraph;

/*        try {
            socketToCTRL = sd;
            os_ = new ObjectOutputStream(sd.getOutputStream());
//            is_ = new ObjectInputStream(sd.getInputStream());
        }
        catch (IOException e) {
            System.err.println("Failed to get outputStream from socket");
            e.printStackTrace();
            System.exit(1); // fail early
        }*/

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

//    // TODO think about the atomicity of this read?
//    public void sendStatusUpdate(){
//        try {
//            int size = flowGroups.size();
//            if(size == 0){
//                return;
//            }
//            String [] fid = new String[size];
//            double [] transmitted = new double[size];
//            boolean [] isFinished = new boolean[size];
//            int i = 0;
//            for (Map.Entry<String, FlowGroupInfo> entry: flowGroups.entrySet()) {
//                FlowGroupInfo f = entry.getValue();
//
//                fid[i] = f.getID();
//                transmitted[i] = f.getTransmitted();
//                isFinished[i] = f.isFinished();
//
//                i++;
////                    f.set_update_pending(true); // no need?
//            }
//            AgentMessage m = new FlowStatusMessage(size , fid , transmitted , isFinished);
//            sendMsgToCTRL(m);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//
//            return;
//        }
//    }

    public void finishFlowGroup(String fgID){

//        AgentMessage m = new FlowStatusMessage(fgID,-1, true);
//        try {
//            sendMsgToCTRL(m);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        flowGroups.remove(fgID);
    }

//    private synchronized void sendMsgToCTRL(AgentMessage m) throws IOException {
//        os_.writeObject(m);
//    }


    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

//    public HashMap<String, PConnection[]> getConnection_pools_() { return connection_pools_; }

//    public HashMap<String, PConnection> getConnections_() { return connections_; }

    public HashMap<String, FlowGroupInfo> getFlowGroups() { return flowGroups; }
}
