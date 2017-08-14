package gaiasim.gaiaagent;

// For coordinating the workers in the SA.
// serialize the status report from workers.
// decode messages from CTRL.

import gaiasim.gaiaprotos.GaiaMessageProtos;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("Duplicates")

public class CTRLMessageListener implements Runnable{
    private static final Logger logger = LogManager.getLogger();

    LinkedBlockingQueue<GaiaMessageProtos.FlowUpdate> ctrlQueue;
    AgentSharedData agentSharedData;

    public CTRLMessageListener(LinkedBlockingQueue<GaiaMessageProtos.FlowUpdate> controllerQueue, AgentSharedData sharedData) {
        this.ctrlQueue = controllerQueue;
        this.agentSharedData = sharedData;
    }

    @Override
    public void run() {

        while (true){
            try {
                GaiaMessageProtos.FlowUpdate m = ctrlQueue.take();

                // Decodes the message from CTRL and create subscription/unsubscription messages.
                // Goal: subscribe updated rates, and UNSUBSCRIBE ALL OTHER FLOWGROUPS.
/*
                // first reset all current subscription rates: (so we don't need to check if a flow is subscribed)
                for(Map.Entry<String , ArrayList< HashMap<String , SubscriptionInfo>>> entry : agentSharedData.subscriptionRateMaps.entrySet()) { // for all RAs
                    for (HashMap<String, SubscriptionInfo> h : entry.getValue()) { // for each path, clean the hashMap
                        h.forEach((k,v) -> v.setRate(0.0)); // we don't remove, just set the rate to 0.
                    }
                }*/ //

                for( gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate.RAUpdateEntry rau : m.getRAUpdateList() ) {
                    String raID = rau.getRaID();

                    for ( gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate.FlowUpdateEntry fge : rau.getFgesList()) {
                        String fgID = fge.getFlowID();

                        switch (fge.getOp()){
                            case START:

                                agentSharedData.startFlow(raID, fgID, fge);
                                break;

                            case CHANGE:

                                agentSharedData.changeFlow(raID, fgID, fge);
                                break;

                            case PAUSE: // only pause the FG, no rate set
                                agentSharedData.pauseFlow(raID, fgID, fge);
                                break;

                            case UNRECOGNIZED:
                                logger.error("FUM message have unrecognized Op");
                                break;
                        }

                        // add this flowgroup when not existent // only accept volume from CTRL at this point.
                        if( !agentSharedData.flowGroups.containsKey(fgID)){
                            agentSharedData.flowGroups.put(fgID , new FlowGroupInfo(fgID , fge.getRemainingVolume() ) );
                        }

                        //
                        for ( gaiasim.gaiaprotos.GaiaMessageProtos.FlowUpdate.PathRateEntry pathToRate : fge.getPathToRateList() ){
                            int pathID = pathToRate.getPathID();
                            double rate = pathToRate.getRate();
                            ConcurrentHashMap<String, SubscriptionInfo> infoMap = agentSharedData.subscriptionRateMaps.get(raID).get(pathID);

                            if( infoMap.containsKey(fgID)){ // check whether this FlowGroup is in subscriptionMap.
                                infoMap.get(fgID).setRate( rate );
                            }
                            else { // create this info
                                infoMap.put(fgID , new SubscriptionInfo(fgID, agentSharedData.flowGroups.get(fgID) , rate ));
                            }

                        } // end loop for pathID

                    } // end loop for fgID

                } // end loop for raID

                // notify all subscribed workers..? or maybe all workers?
                for( String raID : agentSharedData.subscriptionRateMaps.keySet()) {
                    ArrayList<ConcurrentHashMap<String , SubscriptionInfo> > al = agentSharedData.subscriptionRateMaps.get(raID);
                    for (int i = 0 ; i < al.size() ; i++){ // i = pathID..?
                        agentSharedData.workerQueues.get(raID)[i].put( new SubscriptionMessage());
                    }
                }


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
