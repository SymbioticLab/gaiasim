package gaiasim.gaiaagent;

// For coordinating the workers in the SA.
// serialize the status report from workers.
// decode messages from CTRL.


import gaiasim.gaiamessage.FlowUpdateMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CTRLMessageListener implements Runnable{
    LinkedBlockingQueue<ControlThreadMessage> ctrlQueue;
    SharedInterface si;
    private static final Logger logger = LogManager.getLogger();

    public CTRLMessageListener(LinkedBlockingQueue<ControlThreadMessage> controllerQueue, SharedInterface si) {
        this.ctrlQueue = controllerQueue;
        this.si = si;
    }

    @Override
    public void run() {

        while (true){
            try {
                ControlThreadMessage m = ctrlQueue.take();

                switch (m.getType()){
                    case FUM:
                        // Decodes the message from CTRL and create subscription/unsubscription messages.
                        // Goal: subscribe updated rates, and UNSUBSCRIBE ALL OTHER FLOWGROUPS.

                        // first reset all current subscription rates: (so we don't need to check if a flow is subscribed)
                        for(Map.Entry<String , ArrayList< HashMap<String , SubscriptionInfo>>> entry : si.subscriptionRateMaps.entrySet()) { // for all RAs
                            for (HashMap<String, SubscriptionInfo> h : entry.getValue()) { // for each path, clean the hashMap
                                h.forEach((k,v) -> v.setRate(0.0)); // we don't remove, just set the rate to 0.
                            }
                        }

//                        System.out.println("CTRL: Received FUM. ");
                        FlowUpdateMessage fum = m.fum; // this time the message contains a lot of information
                        HashMap<String, HashMap<String, FlowUpdateMessage.FlowGroupEntry>> map = fum.getContent();

                        logger.info("Received FUM {}");
                        for (Map.Entry<String, HashMap<String, FlowUpdateMessage.FlowGroupEntry>> oe: map.entrySet() ){
                            String raID = oe.getKey();

                            for (Map.Entry<String, FlowUpdateMessage.FlowGroupEntry> ie : oe.getValue().entrySet()){
                                String fgID = ie.getKey();

                                // add this flowgroup when not existent // only accept volume from CTRL at this point.
                                if( !si.flowGroups.containsKey(fgID)){
                                    si.flowGroups.put(fgID , new FlowGroupInfo(fgID , ie.getValue().remainingVolume ) );
                                }

                                for (Map.Entry<Integer , Double> entry: ie.getValue().pathToRate.entrySet()){
                                    int pathID = entry.getKey();
                                    double rate = entry.getValue();
                                    HashMap<String, SubscriptionInfo> infoMap = si.subscriptionRateMaps.get(raID).get(pathID);

                                    if( infoMap.containsKey(fgID)){ // check whether this FlowGroup is in subscriptionMap.
                                        infoMap.get(fgID).setRate( rate );
                                    }
                                    else { // create this info
                                        infoMap.put(fgID , new SubscriptionInfo(fgID, si.flowGroups.get(fgID) , entry.getValue() ));
                                    }

                                } // end loop for pathID

                            } // end loop for fgID

                        } // end loop for raID

                        // notify all subscribed workers..? or maybe all workers?
                        for( String raID : si.subscriptionRateMaps.keySet()) {
                            ArrayList<HashMap<String , SubscriptionInfo> > al = si.subscriptionRateMaps.get(raID);
                            for (int i = 0 ; i < al.size() ; i++){ // i = pathID..?
                                si.workerQueues.get(raID)[i].put( new SubscriptionMessage());
                            }
                        }

                        break;


                    case DATA_TRANSMITTED:

                        break;
                }
            } catch (InterruptedException e) {
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
