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

import gaiasim.comm.ControlMessage;
import gaiasim.comm.PortAnnouncementMessage_Old;
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

    String id_;
    String trace_id_;
    NetGraph netGraph;
    Configuration config;

    final ScheduledExecutorService statusExec;

    // TODO defined the message.
    LinkedBlockingQueue<WorkerMessage> controllerQueue = new LinkedBlockingQueue<WorkerMessage>();

    // TODO: use the new PA msg.
    public void setupPConns(NetGraph net_graph){
        for (String ra_id : net_graph.nodes_) {

            if (!id_.equals(ra_id)) { // don't consider path to SA itself.

                // because apap is consistent among different programs.
                PConnection[] conns = new PConnection[net_graph.apap_.get(id_).get(ra_id).size()];
                for (int i = 0; i < conns.length; i++) {
                    // ID of connection is SA_id-RA_id.path_id
                    String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);
                    int raID = Integer.parseInt(ra_id);

                    try {
                        // Create the socket that the PersistentConnection object will use
                        Socket socketToRA = new Socket( config.getRAIP(raID) , config.getRAPort(raID));
                        int port = socketToRA.getLocalPort();
                        PConnection conn = new PConnection(conn_id, socketToRA);
                        conns[i] = conn;

                        // fix me. conn ID = conn.data.ID = SA_id-RA_id.path_id
                        saAPI.connections_.put(conn.data_.id_, conn);

                        // Inform the controller of the port number selected
                        saAPI.writeMessage(new PortAnnouncementMessage_Old(id_, ra_id, i, port));
                    }
                    catch (java.io.IOException e) {
                        // TODO: Close socket
                        e.printStackTrace();
                        System.exit(1);
                    }
                }

                saAPI.connection_pools_.put(ra_id, conns);

            } // if id != ra_id

        } // for ra_id in nodes
    }



    @Override
    public void run() {

        final Runnable sendStatus = new Runnable() {
            @Override
            public void run() {
                saAPI.sendStatusUpdate();
            }
        };

        ScheduledFuture<?> mainHandler = statusExec.scheduleAtFixedRate(sendStatus, 0, 200, MILLISECONDS);

        CTRLMessageListener controller = new CTRLMessageListener(controllerQueue);
        controller.run();


    }

    // a small inner class for forwarding socket from CTRL to controller event queue.
    private class CTRLListener implements Runnable{
        ObjectInputStream inputStream;
        public CTRLListener(ObjectInputStream inputStream, LinkedBlockingQueue<WorkerMessage> controllerQueue){
            this.inputStream = inputStream;
        }

        @Override
        public void run() {

            while (true){
                try {
                    ControlMessage c = (ControlMessage) inputStream.readObject();

                    // TODO handle the message and put events into the corresponding SA's event queue.


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

//            ControlMessage c = (ControlMessage) is_.readObject();
//
//            // TODO change the message into the only ONE message that we will be using.
//
//            if (c.type_ == ControlMessage.Type.FLOW_START) {
//                System.out.println(trace_id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
//                assert(!flowGroups.containsKey(c.flow_id_));
//
//                FlowInfo f = new FlowInfo(c.flow_id_, c.field0_, c.field1_, dataBroker);
//                flowGroups.put(f.id_, f);
//            }
//            else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
//                System.out.println(trace_id_ + " FLOW_UPDATE(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
//                assert(flowGroups.containsKey(c.flow_id_));
//                FlowInfo f = flowGroups.get(c.flow_id_);
//                f.update_flow(c.field0_, c.field1_);
//            }
//            else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
//                System.out.println(trace_id_ + " SUBFLOW_INFO(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
//                assert flowGroups.containsKey(c.flow_id_) : trace_id_ + " does not currently have " + c.flow_id_;
//                FlowInfo f = flowGroups.get(c.flow_id_);
//                PersistentConnection conn = connection_pools_.get(c.ra_id_)[c.field0_];
//                f.add_subflow(conn, c.field1_);
//            }
//            else {
//                System.out.println(trace_id_ + " received an unexpected ControlMessage");
////                        System.exit(1);
//            }

        }
    }

    // The constructor is called after the socket is accepted.

    // TODO socket to CTRL should be handled at what level?
    // TODO listens on socket from CTRL, and forward the event into controllerQueue

    public PersistentSA_New(String id, NetGraph net_graph, Socket client_sd , Configuration configuration) {

        try{
            ObjectInputStream is_ = new ObjectInputStream(client_sd.getInputStream());
            Thread CTRLForwarder = new Thread(new CTRLListener(is_ , controllerQueue));

            CTRLForwarder.start();

        } catch (IOException e) {
            e.printStackTrace();
        }

        saAPI = new SharedInterface(id , client_sd);
        netGraph = net_graph;

        statusExec = Executors.newScheduledThreadPool(1);

        config = configuration;

        trace_id_ = Constants.node_id_to_trace_id.get(id);


        // First set up Persistent Connections and send PA Messages.
        setupPConns(netGraph);

        // start the worker thread.
    }

    // called periodically to send the HeartBeat STATUS message to GAIA CTRL.
    public void sendHeatBeat(){

    }

}
