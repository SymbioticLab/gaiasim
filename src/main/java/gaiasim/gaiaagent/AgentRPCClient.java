package gaiasim.gaiaagent;

// RPC client on the agent side, to send status update message to master.

import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.gaiaprotos.MasterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AgentRPCClient {
    private static final Logger logger = LogManager.getLogger();

    AgentSharedData sharedData;

    private final ManagedChannel channel;
//    private final MasterServiceGrpc.MasterServiceBlockingStub blockingStub;
    private final MasterServiceGrpc.MasterServiceStub asyncStub;
    private StreamObserver<GaiaMessageProtos.FlowStatus_ACK> responseObserver;
    // should not create a new stream every time!!!
    StreamObserver<GaiaMessageProtos.StatusReport> clientStreamObserver;

    public AgentRPCClient (String masterIP, int masterPort, AgentSharedData sharedData) {
        this(ManagedChannelBuilder.forAddress(masterIP, masterPort).usePlaintext(true).build());
        this.sharedData = sharedData;
        logger.info("Agent RPC Client connecting to {}:{}", masterIP, masterPort);

        responseObserver = new StreamObserver<GaiaMessageProtos.FlowStatus_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FlowStatus_ACK flowStatus_ack) {
                logger.info("Received flowStatus_ack from server");
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
            }
        };


        // FIXME: when to call this function? after we are sure that the master is up!
        clientStreamObserver = asyncStub.updateFlowStatus(responseObserver);
    }

    public AgentRPCClient(ManagedChannel channel) {
        this.channel = channel;
        this.asyncStub = MasterServiceGrpc.newStub(channel);
//        blockingStub = MasterServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void sendStatusUpdate() {

        int size = sharedData.flowGroups.size();
        if(size == 0){
//            System.out.println("FG_SIZE = 0");
            return;         // if there is no data to send (i.e. the master has not come online), we simply skip.
        }

//        GaiaMessageProtos.StatusReport statusReport = statusReportBuilder.build();
        GaiaMessageProtos.StatusReport statusReport = buildStatusReport();

        clientStreamObserver.onNext(statusReport);

        logger.info("finished sending status report\n{}", statusReport);
    }

    private GaiaMessageProtos.StatusReport buildStatusReport() {

        GaiaMessageProtos.StatusReport.Builder statusReportBuilder = GaiaMessageProtos.StatusReport.newBuilder();

        for (Map.Entry<String, FlowGroupInfo> entry: sharedData.flowGroups.entrySet()) {
            FlowGroupInfo f = entry.getValue();

            GaiaMessageProtos.StatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.StatusReport.FlowStatus.newBuilder()
                    .setFinished(f.isFinished()).setId(f.getID()).setTransmitted(f.getTransmitted());

            statusReportBuilder.addStatus(fsBuilder);
        }

        return statusReportBuilder.build();
    }

    public void testStatusUpdate(){
        GaiaMessageProtos.StatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.StatusReport.FlowStatus.newBuilder()
                .setFinished(false).setId("test").setTransmitted(10);
        GaiaMessageProtos.StatusReport.Builder statusReportBuilder = GaiaMessageProtos.StatusReport.newBuilder();
        statusReportBuilder.addStatus(fsBuilder);

        GaiaMessageProtos.StatusReport statusReport = statusReportBuilder.build();

        clientStreamObserver.onNext(statusReport);

        logger.info("finished testing status report");
    }

    public void sendFG_FIN(String fgID){
        GaiaMessageProtos.StatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.StatusReport.FlowStatus.newBuilder()
                .setFinished(true).setId(fgID).setTransmitted(0);
//        GaiaMessageProtos.StatusReport.Builder statusReportBuilder = GaiaMessageProtos.StatusReport.newBuilder().addStatus(fsBuilder);
//        statusReportBuilder.addStatus(fsBuilder);

        GaiaMessageProtos.StatusReport FG_FIN = GaiaMessageProtos.StatusReport.newBuilder().addStatus(fsBuilder).build();

        clientStreamObserver.onNext(FG_FIN);

//        logger.info("finished sending FLOW_FIN for {}", fgID);
    }
}