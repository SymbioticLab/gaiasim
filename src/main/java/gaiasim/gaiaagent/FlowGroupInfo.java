package gaiasim.gaiaagent;

// FlowGroup Info contains the runtime information (remaining volume, rate etc.)
// it is created by SAs.

// Life Cycle: created ->

import gaiasim.util.Constants;

public class FlowGroupInfo {

    final String ID;
    final double volume;

    volatile boolean finished = false;
    volatile double transmitted = 0.0;

    public FlowGroupInfo(String ID, double volume) {
        this.ID = ID;
        this.volume = volume;
        this.finished = false;
    }


    // Getters.

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
}
