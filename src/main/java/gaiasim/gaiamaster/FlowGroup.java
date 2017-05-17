package gaiasim.gaiamaster;

// New definition of FlowGroup

import gaiasim.network.Pathway;

import java.util.ArrayList;

public class FlowGroup {

    // final fields
    private final String id;

    private final String src;

    private final String dst;
    private final String owningCoflowID;
    private final double totalVolume;
    // non-final fields
    private long startTime = -1;

    private long endTime = -1;
    // make this field volatile!
    private volatile double transmitted;

    // The subflow info, is essientially immutable data.
    private ArrayList<Pathway> paths = new ArrayList<Pathway>();

    public FlowGroup(String id, String src, String dst, String owningCoflowID, double totalVolume) {
        this.id = id;
        this.src = src;
        this.dst = dst;
        this.owningCoflowID = owningCoflowID;
        this.totalVolume = totalVolume;
    }


    public String getId() { return id; }

    public double getTotalVolume() { return totalVolume; }

    public String getSrc() { return src; }

    public String getDst() { return dst; }

    public String getOwningCoflowID() { return owningCoflowID; }

}
