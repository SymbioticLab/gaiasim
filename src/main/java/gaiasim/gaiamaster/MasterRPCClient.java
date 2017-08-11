package gaiasim.gaiamaster;


import gaiasim.gaiaprotos.GaiaMessageProtos;
import gaiasim.gaiaprotos.SendingAgentServiceGrpc;
import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MasterRPCClient {

    private static final Logger logger = LogManager.getLogger();

    private final ManagedChannel channel;
    private final SendingAgentServiceGrpc.SendingAgentServiceBlockingStub blockingStub;
    private final SendingAgentServiceGrpc.SendingAgentServiceStub asyncStub;
    private final StreamObserver<GaiaMessageProtos.FUM_ACK> responseObserver;
    private final StreamObserver<GaiaMessageProtos.FlowUpdate> clientStreamObserver;

    String targetIP;
    int targetPort;

    public MasterRPCClient(String saIP, int saPort) {
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        this(ManagedChannelBuilder.forAddress(saIP, saPort).usePlaintext(true).build());
        this.targetIP = saIP;
        this.targetPort = saPort;
    }

    public MasterRPCClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = SendingAgentServiceGrpc.newBlockingStub(channel);

        asyncStub = SendingAgentServiceGrpc.newStub(channel);
        responseObserver = new StreamObserver<GaiaMessageProtos.FUM_ACK>() {

            @Override
            public void onNext(GaiaMessageProtos.FUM_ACK fumAck) {
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

        clientStreamObserver = asyncStub.changeFlow(responseObserver);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public Iterator<GaiaMessageProtos.PAMessage> preparePConn(){
        GaiaMessageProtos.PAM_REQ req = GaiaMessageProtos.PAM_REQ.newBuilder().build();
        return blockingStub.prepareConnections(req);
    }

    public void setFlow( Collection<FlowGroup_Old> fgos, NetGraph ng, String saID ){

        GaiaMessageProtos.FlowUpdate fum = buildFUM(fgos, ng, saID);
//        logger.info("Built the FUM\n {}", fum);

        clientStreamObserver.onNext(fum);
        logger.info("FUM sent for saID = {}" , saID);

    }

    public GaiaMessageProtos.FlowUpdate buildFUM(Collection<FlowGroup_Old> fgos, NetGraph ng, String saID){

        GaiaMessageProtos.FlowUpdate.Builder fumBuilder = GaiaMessageProtos.FlowUpdate.newBuilder();

        // first sort all fgos according to the RA.
        Map< String , List<FlowGroup_Old>> fgobyRA = fgos.stream().collect(Collectors.groupingBy(FlowGroup_Old::getDst_loc));

        for (Map.Entry<String , List<FlowGroup_Old>> entrybyRA: fgobyRA.entrySet()) {

//            String raID = entrybyRA.getKey();

            GaiaMessageProtos.FlowUpdate.RAUpdateEntry.Builder raueBuilder = GaiaMessageProtos.FlowUpdate.RAUpdateEntry.newBuilder();
            raueBuilder.setRaID(entrybyRA.getKey());

            for (FlowGroup_Old fgo : entrybyRA.getValue()) { // for each FGO of this RA, we create an FlowUpdateEntry
                assert (saID.equals(fgo.getSrc_loc()));
                String fgoID = fgo.getId();

                GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.Builder fueBuilder = GaiaMessageProtos.FlowUpdate.FlowUpdateEntry.newBuilder();
                fueBuilder.setRemainingVolume(fgo.remaining_volume());
                fueBuilder.setFlowID(fgoID);

//            HashMap<Object, Object> pathToRate = new HashMap<>();
                for (Pathway p : fgo.paths) {
                    int pathID = ng.get_path_id(p);
                    if (pathID != -1) {
                        fueBuilder.addPathToRate(GaiaMessageProtos.FlowUpdate.PathRateEntry.newBuilder().setPathID(pathID).setRate(p.getBandwidth()));

//                    pathToRate.put(pathID, p.getBandwidth());
                    } else {
                        System.err.println("FATAL: illegal path!");
//                    System.exit(1); // don't fail yet!
                    }
                }

                raueBuilder.addFges(fueBuilder);

            } // end of creating all the FlowUpdateEntry

            fumBuilder.addRAUpdate(raueBuilder);

        } // end of creating all the RAUpdateEntry

        return fumBuilder.build();
    }

}