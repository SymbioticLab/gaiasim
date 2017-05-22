package gaiasim.gaiaagent;

// The abstract class for messages sent into Workers's event queue.


import gaiasim.gaiamessage.FlowUpdateMessage;

public class ControlThreadMessage {

    public enum Type{
        FUM,
        DATA_TRANSMITTED // not using this now, directly write to shared mem.
    }

    protected Type type;
    protected FlowUpdateMessage fum;

    // How to report back the progress? every transmit?


    public ControlThreadMessage(FlowUpdateMessage fum) {
        type = Type.FUM;
        this.fum = fum;
    }

    public Type getType() { return type; }
}
