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
    public String ra_id_;    // ID of the receiving agent that is destination
                             //     of this flow. Only used for SUBFLOW_INFO.
    public int field0_;      // Either number of subflows or path_id
    public double field1_;   // Either flow volume or subflow rate

    public ControlMessage() {}
    public ControlMessage(Type t) {
        type_ = t;
    }
}
