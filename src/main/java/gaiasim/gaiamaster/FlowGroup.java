package gaiasim.gaiamaster;

// New definition of FlowGroup

import gaiasim.network.FlowGroup_Old;
import gaiasim.network.Pathway;

import java.util.ArrayList;

public class FlowGroup {

    // final fields
    private final String id;
    private final String srcLocation;
    private final String dstLocation;
    private final String owningCoflowID;
    private final double totalVolume;

    // non-final fields
    private long startTime = -1;
    private long endTime = -1;

    // make this field volatile! Or maybe atomic?
    private volatile double transmitted;

    // The subflow info, is essientially immutable data? Nope. TODO: where to store this info? Could use a map in GAIA.
    private ArrayList<Pathway> paths = new ArrayList<Pathway>();

    public FlowGroup(String id, String srcLocation, String dstLocation, String owningCoflowID, double totalVolume) {
        this.id = id;
        this.srcLocation = srcLocation;
        this.dstLocation = dstLocation;
        this.owningCoflowID = owningCoflowID;
        this.totalVolume = totalVolume;
    }

    public String getId() { return id; }

    public double getTotalVolume() { return totalVolume; }

    public String getSrcLocation() { return srcLocation; }

    public String getDstLocation() { return dstLocation; }

    public String getOwningCoflowID() { return owningCoflowID; }

    public double getTransmitted() { return transmitted; }

    // TODO check the two converters
    public static FlowGroup_Old toFlowGroup_Old(FlowGroup fg , int intID ){
        FlowGroup_Old fgo = new FlowGroup_Old(fg.getId() , intID ,
                fg.getOwningCoflowID() , fg.getSrcLocation(), fg.getDstLocation() , fg.getTransmitted());

        return fgo;
    }

    public FlowGroup( FlowGroup_Old fgo){
        this.id = fgo.getId();
        this.srcLocation = fgo.getSrc_loc();
        this.dstLocation = fgo.getDst_loc();
        this.owningCoflowID = fgo.getCoflow_id();
        this.totalVolume = fgo.getVolume();
        this.transmitted = fgo.getTransmitted_volume();
    }
}
