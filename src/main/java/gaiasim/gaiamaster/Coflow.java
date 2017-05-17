package gaiasim.gaiamaster;

// The new coflow definition. used by GAIA master, YARN emulator etc.

import java.util.ArrayList;

public class Coflow {
    // final fields
    private final String id;


    // list of flowgroups: final? ArrayList or ConcurrentHashMap?
    private ArrayList<FlowGroup> flowGroups;

    // Optional field
//    private int state;
//    private String owningClient;


    public Coflow(String id, ArrayList<FlowGroup> flowGroups) {

        this.id = id;
        this.flowGroups = flowGroups;
    }

    public String getId() { return id; }
    public ArrayList<FlowGroup> getFlowGroups() { return flowGroups; }
}
