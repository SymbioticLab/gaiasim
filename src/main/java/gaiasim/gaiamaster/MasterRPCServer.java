package gaiasim.gaiamaster;

// receive the status update message from the clients and put them into a queue for serialization

import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.gaiaprotos.MasterServiceGrpc;
import gaiasim.spark.YARNMessages;
import gaiasim.util.Configuration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class MasterRPCServer {
    private static final Logger logger = LogManager.getLogger();

    private Server server;
    int port;
    MasterSharedData masterSharedData;

    public MasterRPCServer(Configuration config, MasterSharedData masterSharedData) {
        this.port = config.getMasterPort();
        this.masterSharedData = masterSharedData;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MasterServiceImpl())
                .build()
                .start();
        logger.info("gRPC Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down master gRPC server since JVM is shutting down");
                MasterRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        }); // end of Shutdown Hook

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

    class MasterServiceImpl extends MasterServiceGrpc.MasterServiceImplBase {

        @Override
        public io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.StatusReport> updateFlowStatus(
                io.grpc.stub.StreamObserver<gaiasim.gaiaprotos.GaiaMessageProtos.FlowStatus_ACK> responseObserver) {

            return new StreamObserver<GaiaMessageProtos.StatusReport>() {
                @Override
                public void onNext(GaiaMessageProtos.StatusReport statusReport) {
                    logger.info("Received Flow Status: {}" , statusReport);
                    handleStatusReport(statusReport);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("ERROR in handling flow status report");
                }

                @Override
                public void onCompleted() {
                    GaiaMessageProtos.FlowStatus_ACK ack = GaiaMessageProtos.FlowStatus_ACK.newBuilder().build();

                    responseObserver.onNext(ack);
                    responseObserver.onCompleted();
                }
            };

        }

    }

    public void handleStatusReport(GaiaMessageProtos.StatusReport statusReport){

        for ( GaiaMessageProtos.StatusReport.FlowStatus status : statusReport.getStatusList()) {
            // first get the current flowGroup ID
            String fid = status.getId();
            if(status.getFinished()){
                onFinishFlowGroup( fid , System.currentTimeMillis());
                continue;
            } else {
                // set the transmitted.
                FlowGroup fg = masterSharedData.getFlowGroup(fid);
                if(fg != null){
                    fg.setTransmitted( status.getTransmitted() );
                }
                else{
                    logger.warn("Received status report but the FlowGroup does not exist");
                }
            }
        }

    }

    private void onFinishFlowGroup(String fid, long timestamp) {

        logger.info("Received FLOW_FIN for {}", fid);
        // set the current status

        masterSharedData.onFinishFlowGroup(fid, timestamp);


    }

}
