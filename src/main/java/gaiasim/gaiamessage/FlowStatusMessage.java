package gaiasim.gaiamessage;

// Heartbeat message from sending agents to report all flow progress.

public class FlowStatusMessage extends AgentMessage{



    public FlowStatusMessage(){
        this.type = Type.FLOW_STATUS;


    }

}
