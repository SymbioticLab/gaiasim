package gaiasim.gaiamaster;

import gaiasim.comm.PortAnnouncementMessage_Old;
import gaiasim.comm.ScheduleMessage;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.gaiamessage.FlowUpdateMessage;
import gaiasim.network.NetGraph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

// A SendingAgentContact facilitates communication between
// the GAIA controller and a sending agent at a certain datacenter.

// we have n SAIs in a GAIA controller.


public class SendingAgentInterface {
    // ID of the sending agent with which this contact communicates
    public String id_;

    // Queue of messages between controller and SendingAgentContact.
    // The SendingAgentContact should only place messages on the queue.
    public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;

    public NetGraph net_graph_;

    public boolean is_baseline_;

    public Socket sd_;

    public ObjectOutputStream os_;

    public Thread listen_sa_thread_;


    private class SendingAgentListener implements Runnable {
        public String id_;
        public Socket sd_; // TCP socket to SendingAgent
        public ObjectInputStream is_;
        //        public LinkedBlockingQueue<ScheduleMessage> schedule_queue_;
        public LinkedBlockingQueue<PortAnnouncementMessage_Old> port_announce_queue_;
        public int num_port_announcements_;
        public boolean is_baseline_;

        public Master.MasterState ms;


        public SendingAgentListener(String id, Socket sd,
                                    LinkedBlockingQueue<PortAnnouncementMessage_Old> port_announce_queue, Master.MasterState ms ,
                                    int num_port_announcements, boolean is_baseline) {
            id_ = id;
            sd_ = sd;
            this.ms = ms;
            port_announce_queue_ = port_announce_queue;
            num_port_announcements_ = num_port_announcements;
            is_baseline_ = is_baseline;

            try {
                is_ = new ObjectInputStream(sd_.getInputStream());
            }
            catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        // Receive messages from the SendingAgent (via socket) and relay them to the
        // controller (via queues). First receive all PortAnnouncements needed by
        // the controller to set up routes for this SendingAgent's paths. Then
        // receive update messages from the SendingaAgents
        public void run() {
            try {
                if (!is_baseline_) {
                    int num_ports_recv = 0;
                    while (num_ports_recv < num_port_announcements_) {
                        PortAnnouncementMessage_Old m = (PortAnnouncementMessage_Old) is_.readObject();
                        port_announce_queue_.put(m);
                        num_ports_recv++;
                        System.out.println("SAI: " + id_ + " Received " + num_ports_recv + " / " + num_port_announcements_);
                    }
                    System.out.println("SAI: " + id_ + " Received all PortAnnouncement Message, now entering main event loop.");
                }
                while (true) { // receives heartbeat message
                    AgentMessage m = (AgentMessage) is_.readObject();
                    // TODO handle heartbeat message
                    try {
                        switch (m.getType()){
                            case FLOW_STATUS:
                                FlowStatusMessage fsm = (FlowStatusMessage) m;
                                if(fsm.getSize() == 1 && fsm.getIsFinished()[0]){ // single FLOW_FIN message
                                    onFinishFlowGroup(fsm.getId()[0] , System.currentTimeMillis());
                                }
                                else { // FLOW_STATUS message.
                                    int size = fsm.getSize();
                                    for(int i = 0 ; i < size ; i++ ){
                                        // first get the current flowGroup ID
                                        String fid = fsm.getId()[i];
                                        if(fsm.getIsFinished()[i]){
                                            onFinishFlowGroup( fid , System.currentTimeMillis());
                                            continue;
                                        } else {
                                            // set the transmitted.
                                            FlowGroup fg = ms.getFlowGroup(fid);
                                            fg.setTransmitted( fsm.getTransmitted()[i] );
                                        }

                                        // anything else?
                                    }
                                }

                                break;

                            case PORT_ANNOUNCEMENT:
                                System.out.println("SAI: should not receive PA during normal operation");
                                break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            catch (java.net.SocketException e) {
                // If the exception is because we closed the socket, we're all good.
                // Otherwise, probably some error.
                if (e.getMessage().contains("Socket closed")) {
                    System.out.println(id_ + " connection to SA closed and interrupted=" + Thread.interrupted());
                    return;
                }

                e.printStackTrace();
                System.exit(1);
            }
            catch (java.io.EOFException e) {
                System.out.println(id_ + " connection to SA closed and interrupted=" + Thread.interrupted());
                return;
            }
            catch (Exception e) {
                e.printStackTrace();
                // TODO: Close socket
                System.exit(1);
            }
        } // end of run()

        // Maybe also a Batch version of onFinish Flow?
        private void onFinishFlowGroup(String id, long curTime) {
            System.out.println("SAI: received FLOW_FIN for " + id);
            // set the current status

            FlowGroup fg = ms.getFlowGroup(id); // TODO: what if this returns null?
            if(fg.getAndSetFinish(curTime)){
                return; // if already finished, do nothing.
            }

            ms.flag_FG_FIN = true;

            // check if the owning coflow is finished
            Coflow cf = ms.coflowPool.get(fg.getOwningCoflowID());

            if(cf == null){ // cf may already be finished.
                return;
            }

            boolean flag = true;

            // TODO verify concurrency issues here. here cf may be null.
            for(FlowGroup ffg : cf.getFlowGroups().values()){
                flag = flag && ffg.isFinished();
            }

            // if so set coflow status, send COFLOW_FIN
            if (flag){
                ms.onFinishCoflow(fg.getOwningCoflowID());
            }


        }

    }

    public SendingAgentInterface(String id, NetGraph netGraph, String sa_ip, int sa_port,
                                 LinkedBlockingQueue<PortAnnouncementMessage_Old> paEventQueue, Master.MasterState ms, boolean enablePersistentConn) {

        id_ = id;
        net_graph_ = netGraph;
//        schedule_queue_ = schedule_queue;
        is_baseline_ = !enablePersistentConn;

        // Determine the number of port announcements that should be
        // received from the SendingAgent.
        int num_port_announcements = 0;
        for (String ra_id : net_graph_.apap_.get(id_).keySet()) {
            num_port_announcements += net_graph_.apap_.get(id_).get(ra_id).size();
        }
        System.out.println("SA " + id_ + " should receive " + num_port_announcements + " port announcements");

        // Open connection with sending agent and
        // get the port numbers that it plans to use
        try {
            sd_ = new Socket(sa_ip, sa_port);
            os_ = new ObjectOutputStream(sd_.getOutputStream());
            System.out.println(id_ + " connected to SA");
        }
        catch (Exception e) {
            System.out.println("ERROR trying to connect to " + sa_ip + ":" + sa_port);
            e.printStackTrace();
            System.exit(1);
        }
        
        // Start thread to listen for messages from the
        // sending agent and add them to the queue.
        listen_sa_thread_ = new Thread(new SendingAgentListener(id_, sd_, paEventQueue, ms,
                                                                num_port_announcements, is_baseline_));
        listen_sa_thread_.start();
    }


    public void sendFlowUpdate_Blocking(FlowUpdateMessage m){
        try {
            os_.writeObject(m);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    // Sends a FLOW_START or FLOW_UPDATE  message to the sending agent along
//    // with information about the paths and rates used by the flow.
//    public void start_flow(FlowGroup_Old f) {
//        String start_or_update = f.isUpdated() ? "UPDATING" : "STARTING";
//        System.out.println(start_or_update + " Flow " + f.getId() + " at " + Constants.node_id_to_trace_id.get(id_));
//
//        ControlMessage c = new ControlMessage();
//        c.type_ = f.isUpdated() ? ControlMessage.Type.FLOW_UPDATE : ControlMessage.Type.FLOW_START;
//        c.flow_id_ = f.getId();
//        c.field0_ = f.paths.size();
//        c.field1_ = f.remaining_volume();
//        c.ra_id_ = f.paths.get(0).dst();
//
//        try {
//            os_.writeObject(c);
//
//            // Only send subflow info if running baseline
//            if (!is_baseline_) {
//                // Send information about each Subflow of the FlowGroup
//                for (Pathway p : f.paths) {
//                    ControlMessage sub_c = new ControlMessage();
//                    sub_c.type_ = ControlMessage.Type.SUBFLOW_INFO;
//                    sub_c.flow_id_ = f.getId();
//                    sub_c.ra_id_ = p.dst();
//                    sub_c.field0_ = net_graph_.get_path_id(p); // TODO: Store this with the path to reduce repeated call -> pass this on to floodlight?
//                    sub_c.field1_ = p.getBandwidth();
//
//                    os_.writeObject(sub_c);
//                }
//            }
//        }
//        catch (java.io.IOException e) {
//            // TODO: Close socket
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
    
}
