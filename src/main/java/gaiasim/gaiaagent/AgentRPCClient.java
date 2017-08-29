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

    AgentSharedData agentSharedData;

    private final ManagedChannel channel;
//    private final MasterServiceGrpc.MasterServiceBlockingStub blockingStub;
    private final MasterServiceGrpc.MasterServiceStub asyncStub;
    private StreamObserver<GaiaMessageProtos.FlowStatus_ACK> responseObserver;
    // should not create a new stream every time!!!
    StreamObserver<GaiaMessageProtos.StatusReport> clientStreamObserver;

    volatile boolean isStreamReady = false;

    public AgentRPCClient (String masterIP, int masterPort, AgentSharedData sharedData) {
        this(ManagedChannelBuilder.forAddress(masterIP, masterPort).usePlaintext(true).build());
        this.agentSharedData = sharedData;
        logger.info("Agent RPC Client connecting to {}:{}", masterIP, masterPort);

        responseObserver = new StreamObserver<GaiaMessageProtos.FlowStatus_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FlowStatus_ACK flowStatus_ack) {
                logger.info("Received flowStatus_ack from server");
            }

            @Override
            public void onError(Throwable t) {
                logger.error("ERROR in agent {} when sending flow status update: {}", agentSharedData.saID, t.toString());
                t.printStackTrace();
                isStreamReady = false;
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
            }
        };

    }

    public AgentRPCClient(ManagedChannel channel) {
        this.channel = channel;
        this.asyncStub = MasterServiceGrpc.newStub(channel);
//        blockingStub = MasterServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void initStream() {
        logger.warn("(Re)starting the Stream for SA {}", agentSharedData.saID);
        clientStreamObserver = asyncStub.updateFlowStatus(responseObserver);
        isStreamReady = true;
    }

    public void sendStatusUpdate() {

        int size = agentSharedData.flowGroups.size();
        if(size == 0){
//            System.out.println("FG_SIZE = 0");
            return;         // if there is no data to send (i.e. the master has not come online), we simply skip.
        }

//        GaiaMessageProtos.StatusReport statusReport = statusReportBuilder.build();
        GaiaMessageProtos.StatusReport statusReport = buildStatusReport();

//        while ( !isStreamReady ) {
//            initStream();
//            clientStreamObserver.onNext(statusReport);
//        }

        if ( !isStreamReady ) {
            initStream();
        }
        synchronized (this) {
            clientStreamObserver.onNext(statusReport);
        }

        logger.debug("finished sending status report\n{}", statusReport);

//        agentSharedData.printSAStatus();

    }

    private GaiaMessageProtos.StatusReport buildStatusReport() {

        GaiaMessageProtos.StatusReport.Builder statusReportBuilder = GaiaMessageProtos.StatusReport.newBuilder();

        for (Map.Entry<String, FlowGroupInfo> entry: agentSharedData.flowGroups.entrySet()) {
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

            GaiaMessageProtos.StatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.StatusReport.FlowStatus.newBuilder()
                    .setFinished(fgi.isFinished()).setId(fgi.getID()).setTransmitted(fgi.getTransmitted());

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

        if ( !isStreamReady ) {
            initStream();
        }

        synchronized (this) {
            clientStreamObserver.onNext(statusReport);
        }

        logger.info("finished testing status report");
    }

    public void sendFG_FIN(String fgID){

        if (fgID == null){
            System.err.println("fgID = null when sending FG_FIN");
            return;
        }

        GaiaMessageProtos.StatusReport.FlowStatus.Builder fsBuilder = GaiaMessageProtos.StatusReport.FlowStatus.newBuilder()
                .setFinished(true).setId(fgID).setTransmitted(0);
//        GaiaMessageProtos.StatusReport.Builder statusReportBuilder = GaiaMessageProtos.StatusReport.newBuilder().addStatus(fsBuilder);
//        statusReportBuilder.addStatus(fsBuilder);

        GaiaMessageProtos.StatusReport FG_FIN = GaiaMessageProtos.StatusReport.newBuilder().addStatus(fsBuilder).build();


        if ( !isStreamReady ) {
            initStream();
        }

        synchronized (this) {
            clientStreamObserver.onNext(FG_FIN);
        }

//        logger.info("finished sending FLOW_FIN for {}", fgID);
    }

}