package gaiasim.gaiaagent;

// For coordinating the workers in the SA.
// serialize the status report from workers.
// decode messages from CTRL.


import gaiasim.gaiamessage.FlowUpdateMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class CTRLMessageListener implements Runnable{
    LinkedBlockingQueue<ControlThreadMessage> ctrlQueue;
    SharedInterface si;
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
                        for( String raID : si.subscriptionRateMaps.keySet()) {
                            for (HashMap<String, SubscriptionInfo> h : si.subscriptionRateMaps.get(raID)) {
                                for (SubscriptionInfo s : h.values()) {
                                    s.setRate(0.0); // TODO to set or to remove?
                                }
                            }
                        }

                        FlowUpdateMessage fum = m.fum; // this is only for a specific RA_ID.
                        int sizeFG = fum.getSizeOfFlowGroups();
                        int sizePaths = fum.getSizeOfPaths();
                        for (int i = 0 ; i < sizeFG ; i++){
                            String fgid = fum.getFgID()[i];

                            // add this flowgroup is not existent // only accept volume from CTRL at this point.
                            if( !si.flowGroups.containsKey(fgid)){
                                si.flowGroups.put(fgid , new FlowGroupInfo(fgid , fum.getRemaingVolume(i)));
                            }
                            // after this codeblock are we sure that flowGroup must contain fgid? // TODO verify the case with completing a flow.

                            for (int j = 0; j < sizePaths ; j++){
                                HashMap<String, SubscriptionInfo> infoMap = si.subscriptionRateMaps.get(fum.getRaID()).get(j);
                                if( infoMap.containsKey(fgid)){
                                    infoMap.get(fgid).setRate( fum.getRate(i , j) );
                                }
                                else { // create this info
                                    infoMap.put(fgid , new SubscriptionInfo(fgid, si.flowGroups.get(fgid) , fum.getRate(i , j)));
                                }
                            }
                        }

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
