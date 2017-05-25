package gaiasim.gaiaagent;

// The new persistentSendingAgent
// share the API as the old PersistentSendingAgent


// Maintains persistent connections with each receiving agent.
// Each path between the sending agent and receiving agent
// has its own persistent connection -- any time a flow is
// to traverse this path it will use this connection (NOTE: if
// this becomes a scalability concern one can maintain a pool
// of connections for each path rather than a single connection).


// function during normal operation
// (1) send heartbeat message of FLOW_STATUS to CTRL (what about finish message, do we send right away?)
// (2) receive and process FLOW_UPDATE from CTRL
// (3) set proper rate for persistent connections

// Function during bootstrap:
// (1) set up persistent connections.
// (2) Upon accepting the socket from CTRL, send PA messages.


// How do we implement throttled sending?
// How do we implement heartbeat message?
// What about thread safety?

// for now just copy the code and modify.

import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.gaiamessage.FlowUpdateMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


//import org.apache.logging.log4j.Logger;

public class PersistentSA_New implements Runnable{

    // A class for shared saAPI (providing API for the threads.)
    SharedInterface saAPI;

    String saID;
    String trace_id_;
    NetGraph netGraph;
    Configuration config;


    final ScheduledExecutorService statusExec;

    LinkedBlockingQueue<ControlThreadMessage> controllerQueue = new LinkedBlockingQueue<ControlThreadMessage>();






    // TODO: use the new PA msg.
    public void setupPConns(NetGraph net_graph){


        for (String ra_id : net_graph.nodes_) {

            if (!saID.equals(ra_id)) { // don't consider path to SA itself.

                // because apap is consistent among different programs.
                LinkedBlockingQueue[] queues = new LinkedBlockingQueue[net_graph.apap_.get(saID).get(ra_id).size()];
                int pathSize = net_graph.apap_.get(saID).get(ra_id).size();
                for (int i = 0; i < pathSize; i++) {
                    // ID of connection is SA_id-RA_id.path_id
                    String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);
                    int raID = Integer.parseInt(ra_id);

                    try {
                        // Create the socket that the PersistentConnection object will use
                        Socket socketToRA = new Socket( config.getRAIP(raID) , config.getRAPort(raID));
                        int port = socketToRA.getLocalPort();

                        queues[i] = new LinkedBlockingQueue<SubscriptionMessage>();

                        // just start worker thread. PConn is the delegate of both worker thread and socket api.
                        Thread wt = new Thread( new Worker(conn_id, ra_id , i ,socketToRA , queues[i] ,saAPI) );
                        wt.start();
                        // We don't track the thread after starting it.

                        // Inform the controller of the port number selected
                        saAPI.sendPAMessageToCTRL(new PortAnnouncementMessage_Old(saID, ra_id, i, port));
                    }
                    catch (java.io.IOException e) {
                        System.err.println("SA: failed on socket to " + ra_id + " IP: " + config.getRAIP(raID) + " Port: " + config.getRAPort(raID));
                        // TODO: Close socket
                        e.printStackTrace();
                        System.exit(1);
                    }
                }

                saAPI.workerQueues.put(ra_id , queues);

            } // if id != ra_id

        } // for ra_id in nodes
    }



    @Override
    public void run() {

        final Runnable sendStatus = () -> saAPI.sendStatusUpdate();

        ScheduledFuture<?> mainHandler = statusExec.scheduleAtFixedRate(sendStatus, 0, Constants.STATUS_MESSAGE_INTERVAL_MS, MILLISECONDS);

        CTRLMessageListener controller = new CTRLMessageListener(controllerQueue, saAPI);
        controller.run();


    }

    // a small inner class for forwarding socket from CTRL to controller event queue.
    private class CTRLListener implements Runnable{
        ObjectInputStream inputStream;
        public CTRLListener(ObjectInputStream inputStream, LinkedBlockingQueue<ControlThreadMessage> controllerQueue){
            this.inputStream = inputStream;
        }

        @Override
        public void run() {

            while (true){
                try {
                    FlowUpdateMessage fum = (FlowUpdateMessage) inputStream.readObject();
                    // simply forwards the msg
                    controllerQueue.put( new ControlThreadMessage(fum) );

                } catch (IOException | ClassNotFoundException | InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1); // fail if happens
                }
            }

        }
    }

    // The constructor is called after the socket is accepted.

    // TODO socket to CTRL should be handled at what level?
    // TODO listens on socket from CTRL, and forward the event into controllerQueue

    public PersistentSA_New(String id, NetGraph net_graph, Socket client_sd , Configuration configuration) {
        this.saID = id;

        try{
            ObjectInputStream is_ = new ObjectInputStream(client_sd.getInputStream());
            Thread CTRLForwarder = new Thread(new CTRLListener(is_ , controllerQueue));

            CTRLForwarder.start();

        } catch (IOException e) {
            e.printStackTrace();
        }

        netGraph = net_graph;
        saAPI = new SharedInterface(id , client_sd , netGraph);

        statusExec = Executors.newScheduledThreadPool(1);

        config = configuration;

        trace_id_ = Constants.node_id_to_trace_id.get(id);


        // First set up Persistent Connections and send PA Messages.
        setupPConns(netGraph);

        // start the worker thread.
    }


}
