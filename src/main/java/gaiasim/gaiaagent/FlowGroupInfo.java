package gaiasim.gaiaagent;

// FlowGroup Info contains the runtime information (remaining volume, rate etc.)
// it is created by SAs.

// Life Cycle: created ->

import gaiasim.util.Constants;

import java.util.ArrayList;
import java.util.List;

public class FlowGroupInfo {

    final String ID;
    final double volume;

    volatile boolean finished = false;
    volatile double transmitted = 0.0;

    volatile FlowState flowState = FlowState.INIT;

    List<WorkerInfo> workerInfoList = new ArrayList<>();

    public enum FlowState{
        INIT,
        RUNNING,
        PAUSED,
        FIN
    }

    public class WorkerInfo{
        int pathID;
        String raID;

        public WorkerInfo(String raID, int pathID) {
            this.raID = raID;
            this.pathID = pathID;
        }

        public int getPathID() { return pathID; }

        public String getRaID() { return raID; }
    }

    public FlowGroupInfo(String ID, double volume) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
    }

    public void addWorkerInfo(String raID, int pathID){
        workerInfoList.add( new FlowGroupInfo.WorkerInfo(raID, pathID));
    }

    public String getID() {
        return ID;
    }

    public boolean isFinished() {
        return finished;
    }

    public double getVolume() {
        return volume;
    }

    // subscribed to ? who knows?
    public double getTransmitted() {
        return transmitted;
    }

    public synchronized boolean transmit(double v) {
        if (finished){ // first check if is already finished.
            return true;
        }
        transmitted += v; // so volatile is not enough!
        if (transmitted + Constants.DOUBLE_EPSILON >= volume){
            finished = true;
            return true;
        }
        else {
            return false;
        }
    }

    public FlowState getFlowState() { return flowState; }

    public FlowGroupInfo setFlowState(FlowState flowState) { this.flowState = flowState; return this;}
}
