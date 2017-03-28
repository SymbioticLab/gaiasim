package gaiasim.comm;

import java.io.Serializable;

// Messages being sent from SendingAgentContacts to SendingAgents
// to cooridnate flow status.
public class ControlMessage implements Serializable {
    public enum Type {
        FLOW_START,
        FLOW_UPDATE,
        SUBFLOW_INFO,
        FLOW_STATUS_REQUEST,
        FLOW_STATUS_RESPONSE,
        TERMINATE
    }

    public Type type_;
    public String flow_id_;
    public int field0_;      // Either number of subflows or path_id
    public double field1_;   // Either flow volume or subflow rate
}
