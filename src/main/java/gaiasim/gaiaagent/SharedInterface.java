package gaiasim.gaiaagent;

// This is the shared saAPI inside the Sending Agent

import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SharedInterface {
    final String saID;
    final String saName; // the previous traceID.

    // A Map containing Connections for each path from this SendingAgent to each
    // ReceivingAgent. The first index of the Map is the ReceivingAgent ID. The
    // second index (the index into the array of Connections) is the ID of the
    // path from the SendingAgent to the ReceivingAgent that is used by that
    // PersistentConnection.
    // TODO: Don't just have a single PersistentConnection per path, but a pool of
    //       Connections per path. This could be implemented as a LinkedBlockingQueue
    //       that Connections get cycled through in RR fashion. Or, if one really
    //       wanted to get fancy, Connections could be kept in some order based
    //       on their relative "hottness" -- how warmed up the TCP connection is.
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

    public Socket socketToCTRL;

    public ObjectOutputStream os_;


    public SharedInterface(String saID, Socket sd, NetGraph netGraph) {
        this.saID = saID;
        this.saName = Constants.node_id_to_trace_id.get(saID);
        this.netGraph = netGraph;

        try {
            socketToCTRL = sd;
            os_ = new ObjectOutputStream(sd.getOutputStream());
//            is_ = new ObjectInputStream(sd.getInputStream());
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

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

    // TODO think about the atomicity of this read?
    public void sendStatusUpdate(){
        try {
            int size = flowGroups.size();
            if(size == 0){
                return;
            }
            String [] fid = new String[size];
            double [] transmitted = new double[size];
            boolean [] isFinished = new boolean[size];
            int i = 0;
            for (Map.Entry<String, FlowGroupInfo> entry: flowGroups.entrySet()) {
                FlowGroupInfo f = entry.getValue();

                fid[i] = f.getID();
                transmitted[i] = f.getTransmitted();
                isFinished[i] = f.isFinished();

                i++;
//                    f.set_update_pending(true); // no need?
            }
            AgentMessage m = new FlowStatusMessage(size , fid , transmitted , isFinished);
            sendMsgToCTRL(m);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            // TODO: Close socket
            return;
        }
    }

    public void finishFlowGroup(String fgID){
        // We don't send out ScheduleMessage this time. so that CTRL can decode correctly.
        // Think about flow_status Message. TODO check when this message is sent out.

        AgentMessage m = new FlowStatusMessage(fgID,-1, true);
        try {
            sendMsgToCTRL(m);
        } catch (IOException e) {
            e.printStackTrace();
        }

        flowGroups.remove(fgID);
    }

    private synchronized void sendMsgToCTRL(AgentMessage m) throws IOException {
        os_.writeObject(m);
    }

    // TODO remove this in the future
    public synchronized void sendPAMessageToCTRL(PortAnnouncementMessage_Old m) throws IOException {
        os_.writeObject(m);
    }

    // called periodically to send the HeartBeat STATUS message to GAIA CTRL.
    public void sendHeartBeat(){

    }


    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

//    public HashMap<String, PConnection[]> getConnection_pools_() { return connection_pools_; }

//    public HashMap<String, PConnection> getConnections_() { return connections_; }

    public HashMap<String, FlowGroupInfo> getFlowGroups() { return flowGroups; }

    public Socket getSocketToCTRL() { return socketToCTRL;}

    public ObjectOutputStream getOs_() { return os_; }

//    public ObjectInputStream getIs_() { return is_; }


}
