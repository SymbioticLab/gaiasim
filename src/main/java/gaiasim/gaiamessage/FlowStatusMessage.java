package gaiasim.gaiamessage;

// Heartbeat message from sending agents to report all flow progress.
//

import java.util.HashMap;

public class FlowStatusMessage extends AgentMessage{

    // the payload, use arrays as the payload
//    private HashMap<String , Double> flow_to_progress_map;
    int size;
    String  [] id;
    double  [] transmitted;
    boolean [] isFinished;

    public FlowStatusMessage(int size, String[] id, double[] transmitted, boolean[] isFinished) {
        this.type = Type.FLOW_STATUS;
        this.size = size;
        this.id = id;
        this.transmitted = transmitted;
        this.isFinished = isFinished;
    }

    public FlowStatusMessage(String id , double transmitted , boolean isFinished){
        this.type = Type.FLOW_STATUS;
        this.size = 1;
        this.id = new String[]{id};
        this.transmitted = new double[]{transmitted};
        this.isFinished = new boolean[]{isFinished};
    }

}
