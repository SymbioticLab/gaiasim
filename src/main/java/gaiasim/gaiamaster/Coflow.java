package gaiasim.gaiamaster;

// The new coflow definition. used by GAIA master, YARN emulator etc.

import java.util.concurrent.ConcurrentHashMap;

public class Coflow {
    // necessary fields

    private final String id;

    // is the list of flowgroups final?
    private ConcurrentHashMap<Object , FlowGroup> flowGroups;

    public Coflow(String id) {
        this.id = id;
    }


    // Optional field
//    private int state;
//    private String owningClient;


    public String getId() { return id; }

    public ConcurrentHashMap<Object, FlowGroup> getFlowGroups() { return flowGroups; }

}
