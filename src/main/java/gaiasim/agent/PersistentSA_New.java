package gaiasim.agent;

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
import gaiasim.comm.ScheduleMessage;
import gaiasim.gaiamessage.AgentMessage;
import gaiasim.gaiamessage.FlowStatusMessage;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class PersistentSA_New extends PersistentSendingAgent{

    // Inner class DataBroker, function:
    // 1. Upon construction, create persistentConn and send PA messages
    //
    public class NewDataBroker extends DataBroker{

        public NewDataBroker(String id, NetGraph net_graph, Socket sd) {
            super(id , net_graph , sd);
        }

        // the new message interface we use
        public synchronized void writeMessage(AgentMessage m) throws java.io.IOException {
            os_.writeObject(m);
        }

        // We may want to override the get_status()  and  finish_flow().

        @Override
        public synchronized void finish_flow(String flow_id) {
            // We don't send out ScheduleMessage this time. so that CTRL can decode correctly.
            // Think about flow_status Message. TODO check when this message is sent out.

            AgentMessage m = new FlowStatusMessage(flow_id,-1, true);
            try {
                writeMessage(m);
            } catch (IOException e) {
                e.printStackTrace();
            }

            flows_.remove(flow_id);
        }

        // overriden, call this method on every 100 ms.
        @Override
        public synchronized void get_status() {
            try {
                int size = flows_.size();
                if(size == 0){
                    return;
                }
                String [] fid = new String[size];
                double [] transmitted = new double[size];
                boolean [] isFinished = new boolean[size];
                int i = 0;
                for (String k : flows_.keySet()) {
                    FlowInfo f = flows_.get(k);
                    fid[i] = f.id_;
                    transmitted[i] = f.transmitted_;
                    isFinished[i] = f.done_;

                    i++;
//                    f.set_update_pending(true); // no need?
                }
                AgentMessage m = new FlowStatusMessage(size , fid , transmitted , isFinished);
                writeMessage(m);
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
        }

        @Override
        public synchronized void writeMessage(ScheduleMessage m) throws java.io.IOException {
            System.out.println("Should not write this legacy ScheduleMessage");
        }

    } // class DataBroker

    // Listens for CTRL messages, and contains some event handling logic.
    private class NewListener implements Runnable {
        public NewDataBroker dataBroker;

        public NewListener(NewDataBroker dataBroker) {
            this.dataBroker = dataBroker;
        }

        public void run() {
            try {
                while (true) {
                    ControlMessage c = (ControlMessage) dataBroker.is_.readObject();

                    // TODO change the message into the only ONE message that we will be using.

                    if (c.type_ == ControlMessage.Type.FLOW_START) {
                        System.out.println(dataBroker.trace_id_ + " FLOW_START(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(!dataBroker.flows_.containsKey(c.flow_id_));

                        FlowInfo f = new FlowInfo(c.flow_id_, c.field0_, c.field1_, dataBroker);
                        dataBroker.flows_.put(f.id_, f);
                    }
                    else if (c.type_ == ControlMessage.Type.FLOW_UPDATE) {
                        System.out.println(dataBroker.trace_id_ + " FLOW_UPDATE(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert(dataBroker.flows_.containsKey(c.flow_id_));
                        FlowInfo f = dataBroker.flows_.get(c.flow_id_);
                        f.update_flow(c.field0_, c.field1_);
                    }
                    else if (c.type_ == ControlMessage.Type.SUBFLOW_INFO) {
                        System.out.println(dataBroker.trace_id_ + " SUBFLOW_INFO(" + c.flow_id_ + ", " + c.field0_ + ", " + c.field1_ + ")");
                        assert dataBroker.flows_.containsKey(c.flow_id_) : dataBroker.trace_id_ + " does not currently have " + c.flow_id_;
                        FlowInfo f = dataBroker.flows_.get(c.flow_id_);
                        PersistentConnection conn = dataBroker.connection_pools_.get(c.ra_id_)[c.field0_];
                        f.add_subflow(conn, c.field1_);
                    }
                    else {
                        System.out.println(dataBroker.trace_id_ + " received an unexpected ControlMessage");
//                        System.exit(1);
                    }
                }
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
            catch (java.lang.ClassNotFoundException e) {
                e.printStackTrace();
                // TODO: Close socket
                return;
            }
        }
    } // class Listener

    public NewDataBroker dataBroker;

    public PersistentSA_New(String id, NetGraph net_graph, Socket client_sd) {
        super();
        dataBroker = new NewDataBroker(id, net_graph, client_sd);

        final ScheduledExecutorService statusExec;
        statusExec = Executors.newScheduledThreadPool(1);
        final Runnable sendStatus = () -> dataBroker.get_status();
        ScheduledFuture<?> mainHandler = statusExec.scheduleAtFixedRate(sendStatus, 0, 200, MILLISECONDS);

        NewListener listener = new NewListener(dataBroker);
        listener.run();
    }
}
