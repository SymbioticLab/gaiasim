package gaiasim.gaiamessage;

// Heartbeat message from sending agents to report all flow progress.
//

import java.util.HashMap;

public class FlowStatusMessage extends AgentMessage{

    // the payload
    private HashMap<String , Double> flow_to_progress_map;

    public FlowStatusMessage(HashMap<String, Double> flow_to_progress_map){
        this.type = Type.FLOW_STATUS;

        this.flow_to_progress_map = flow_to_progress_map;

    }

    public HashMap<String, Double> getFlow_to_progress_map() {    return flow_to_progress_map;    }

}
