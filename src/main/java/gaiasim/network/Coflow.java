package gaiasim.network;

import java.util.HashMap;

public class Coflow {
    
    public String id_;
    public HashMap<String, Flow> flows_;
    public double volume_;

    public Coflow(String id, double volume, HashMap<String, Flow> flows) {
        id_ = id;
        flows_ = flows;
        volume_ = volume;
    }

    // Return whether owned Flows are done
    public boolean done() {
        for (String k : flows_.keySet()) {
            if (!flows_.get(k).done) {
                return false;
            }
        }

        return true;
    }
}
