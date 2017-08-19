package gaiasim.gaiaagent;

/* states:
 1. idle
 2. connecting to RAs
 3. ready
*/

import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.gaiaprotos.SendingAgentServiceGrpc;
import gaiasim.network.NetGraph;
import gaiasim.util.Configuration;
import gaiasim.util.Constants;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class AgentRPCServer {
    private static final Logger logger = LogManager.getLogger();

    private Server server;
    int port = 23000; // default port number
    NetGraph netGraph;
    String saID;
    String trace_id_;
    Configuration config;

    // data structures from the SAAPI
    AgentSharedData sharedData;
    private int fumaxsize = 0;

    public AgentRPCServer(String id, NetGraph net_graph, Configuration config, AgentSharedData sharedData) {
        this.config = config;
        this.saID = id;
        this.netGraph = net_graph;
        this.port = config.getSAPort(Integer.parseInt(id));
        this.trace_id_ = Constants.node_id_to_trace_id.get(id);
        this.sharedData = sharedData;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new SAServiceImpl())
                .build()
                .start();
        logger.info("gRPC Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                AgentRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        }); // end of Shutdown Hook


        // TODO forward the FUM message

    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    class SAServiceImpl extends SendingAgentServiceGrpc.SendingAgentServiceImplBase {

        // handler of prepareConns message, setup the Workers and PConns, reply with the PA message.
        @Override
        public void prepareConnections(gaiasim.gaiaprotos.GaiaMessageProtos.PAM_REQ request,
                                       io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.PAMessage> responseObserver) {

            if (sharedData.saState != AgentSharedData.SAState.IDLE) {
                logger.error("Received Prepare Connection message when not IDLE");
                // TODO error handling
//                    responseObserver.onError();
            }

            sharedData.saState = AgentSharedData.SAState.CONNECTING;
            // set up Persistent Connections and send PA Messages.
            for (String ra_id : netGraph.nodes_) {

                if (!saID.equals(ra_id)) { // don't consider path to SA itself.

                    // because apap is consistent among different programs.
                    LinkedBlockingQueue[] queues = new LinkedBlockingQueue[netGraph.apap_.get(saID).get(ra_id).size()];
                    int pathSize = netGraph.apap_.get(saID).get(ra_id).size();
                    for (int i = 0; i < pathSize; i++) {
                        // ID of connection is SA_id-RA_id.path_id
                        String conn_id = trace_id_ + "-" + Constants.node_id_to_trace_id.get(ra_id) + "." + Integer.toString(i);
                        int raID = Integer.parseInt(ra_id);

                        try {
                            // Create the socket that the PersistentConnection object will use
                            Socket socketToRA = new Socket( config.getRAIP(raID) , config.getRAPort(raID));
                            socketToRA.setSoTimeout(0);
                            int port = socketToRA.getLocalPort();

                            queues[i] = new LinkedBlockingQueue<SubscriptionMessage>();

                            // send PA message
                            GaiaMessageProtos.PAMessage reply = GaiaMessageProtos.PAMessage.newBuilder().setSaId(saID).setRaId(ra_id).setPathId(i).setPortNo(port).build();
                            responseObserver.onNext(reply);

                            // Start the worker Thread TODO: handle thread failure/PConn failure
                            Thread wt = new Thread( new WorkerThread(conn_id, ra_id , i ,socketToRA , queues[i] , sharedData) );
                            wt.start();

                        }
                        catch (java.io.IOException e) {
                            logger.error("failed on socket to RA {} @ IP: {} Port: {}", ra_id , config.getRAIP(raID), config.getRAPort(raID));
//                                System.err.println("SA: failed on socket to RA " + ra_id + " @IP: " + config.getRAIP(raID) + " Port: " + config.getRAPort(raID));
                            e.printStackTrace();
                            System.exit(1); // fail early
                        }
                    }

                    sharedData.workerQueues.put(ra_id , queues);

                } // if id != ra_id

            } // for ra_id in nodes

            responseObserver.onCompleted();
            sharedData.saState = AgentSharedData.SAState.READY;
            sharedData.readySignal.countDown();
//            agentSharedData.saState.notify();
        }

        @Override
        public io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate> changeFlow(
                io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.FUM_ACK> responseObserver) {

            return new StreamObserver<GaiaMessageProtos.FlowUpdate>() {
                @Override
                public void onNext(GaiaMessageProtos.FlowUpdate flowUpdate) {
                    int fusize = flowUpdate.getSerializedSize();
                    fumaxsize = fusize > fumaxsize ? fusize : fumaxsize;
                    logger.debug("Received FUM, size: {} / {}\ncontent: {}", fusize, fumaxsize, flowUpdate);
                    try {
                        sharedData.fumQueue.put(flowUpdate);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("ERROR in agent {} when handling FUM: {}", sharedData.saID, t.toString());
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    logger.info("Received RPC completion from master RPC client");
                }
            };
        }

        // non-stream version
/*        @Override
        public void changeFlow(gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate request,
                               io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.FUM_ACK> responseObserver) {
            if (saState != SAState.READY) {
                logger.error("Received changeFLow when not READY");
            }

            // forward the FUM to the CTRLMessageListener.
            try {
                fumQueue.put(request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            gaiasim.gaiaprotos.GaiaMessageProtos.FUM_ACK fumAck = gaiasim.gaiaprotos.GaiaMessageProtos.FUM_ACK.newBuilder().build();

            responseObserver.onNext(fumAck);
            responseObserver.onCompleted();

        }*/


    }
}
