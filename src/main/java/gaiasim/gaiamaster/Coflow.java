package gaiasim.gaiamaster;

// The new coflow definition. used by GAIA master, YARN emulator etc.

import gaiasim.network.Coflow_Old;
import gaiasim.network.FlowGroup_Old;
import gaiasim.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Coflow {
    // final fields
    private final String id;


    // list of flowgroups: final? ArrayList or ConcurrentHashMap?
    private HashMap<String , FlowGroup> flowGroups;

    // multiple FGs may finish concurrently leading to the finish of Coflow.
    private AtomicBoolean finished = new AtomicBoolean(false);

    private long startTime = -1;

    private long endTime = -1;

    public Coflow(String id, HashMap<String , FlowGroup> flowGroups) {
        this.id = id;
        this.flowGroups = flowGroups;
    }

    public String getId() { return id; }


    public HashMap<String , FlowGroup>  getFlowGroups() { return flowGroups; }

    public FlowGroup getFlowGroup(String fgid) { return flowGroups.get(fgid); }
    // TODO verify the two converters
    // converter between Old Coflow and new coflow, for use by Scheduler.
    // scheduler takes in ID, flowgroups (with IntID, srcLoc, dstLoc, volume remain.)
    public static Coflow_Old toCoflow_Old_with_Trimming(Coflow cf){
        Coflow_Old ret = new Coflow_Old(cf.getId(), new String[]{"null"}); // location not specified here.

        HashMap<String, FlowGroup_Old> flows = new HashMap<String, FlowGroup_Old>();

        int cnt = 0;
        for (FlowGroup fg : cf.getFlowGroups().values()){
            if(fg.isFinished() || fg.getTransmitted() + Constants.DOUBLE_EPSILON >= fg.getTotalVolume())
            {
                continue;                // Trim the Coflow_Old, so we don't schedule FGs that are already finished.
            }

            FlowGroup_Old fgo = FlowGroup.toFlowGroup_Old(fg, (cnt++));
            flows.put( fg.getId() , fgo);
        }

        ret.flows = flows;

        return ret;
    }
    public Coflow (Coflow_Old cfo){
        this.id = cfo.id;
        this.flowGroups = new HashMap<String , FlowGroup>();
        for(String k : cfo.flows.keySet()){
            FlowGroup fg = new FlowGroup(cfo.flows.get(k));
            flowGroups.put( fg.getId() , fg);
        }

    }

    public boolean getFinished() { return finished.get(); }


    public void setFinished(boolean value) { this.finished.set(value); }

    public boolean getAndSetFinished(boolean newValue) { return this.finished.getAndSet(newValue); }

    //    private int state;
//    private String owningClient;
// Optional field
    public long getStartTime() { return startTime; }

    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }

    public void setEndTime(long endTime) { this.endTime = endTime; }
}
