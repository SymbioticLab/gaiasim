package gaiasim.gaiaagent;

// This is the shared saAPI inside the Sending Agent

import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.util.Constants;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

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
    public HashMap<String, PConnection[]> connection_pools_ = new HashMap<String, PConnection[]>();

    // A Map of all Connections, indexed by PersistentConnection ID. PersistentConnection ID is
    // composed of ReceivingAgentID + PathID.
    public HashMap<String, PConnection> connections_ = new HashMap<String, PConnection>();

    // Flows that are currently being sent by this SendingAgent
    public HashMap<String, FlowGroupInfo> flows_ = new HashMap<String, FlowGroupInfo>();

    public Socket socketToCTRL;

    public ObjectOutputStream os_;


    public SharedInterface(String saID , Socket sd) {
        this.saID = saID;
        this.saName = Constants.node_id_to_trace_id.get(saID);

        try {
            socketToCTRL = sd;
            os_ = new ObjectOutputStream(sd.getOutputStream());
//            is_ = new ObjectInputStream(sd.getInputStream());
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    public void sendStatusUpdate(){
        try {
            int size = flows_.size();
            if(size == 0){
                return;
            }
            String [] fid = new String[size];
            double [] transmitted = new double[size];
            boolean [] isFinished = new boolean[size];
            int i = 0;
            for (String k : flows_.keySet()) {
                FlowGroupInfo f = flows_.get(k);

                fid[i] = f.id_;
                transmitted[i] = f.transmitted_;
                isFinished[i] = f.done_;

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

        flows_.remove(fgID);
    }

    private void sendMsgToCTRL(AgentMessage m) throws IOException {
        os_.writeObject(m);
    }

    // TODO remove this in the future
    public void writeMessage(PortAnnouncementMessage_Old m) throws IOException {
        os_.writeObject(m);
    }


    // Getters//

    public String getSaID() { return saID; }

    public String getSaName() { return saName; }

    public HashMap<String, PConnection[]> getConnection_pools_() { return connection_pools_; }

    public HashMap<String, PConnection> getConnections_() { return connections_; }

    public HashMap<String, FlowGroupInfo> getFlows_() { return flows_; }

    public Socket getSocketToCTRL() { return socketToCTRL;}

    public ObjectOutputStream getOs_() { return os_; }

//    public ObjectInputStream getIs_() { return is_; }


}
