package gaiasim.gaiaagent;

// Message for subscribe a FlowGroup to a persistent connection (worker).

public class SubscriptionMessage {
    public enum  MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        SYNC // SYNC do not carry information
    }

    MsgType type;

    FlowGroupInfo fgi;
    double rate = 0.0;
    boolean pause = false;

    // subscribe or change rate.
    public SubscriptionMessage(FlowGroupInfo fgi, double rate) {
        this.type = MsgType.SUBSCRIBE;
        this.fgi = fgi;
        this.rate = rate;
        this.pause = false;
    }

    // unsubscribe
    public SubscriptionMessage(FlowGroupInfo fgi) {
        this.type = MsgType.UNSUBSCRIBE;
        this.fgi = fgi;
        this.rate = 0.0;
        this.pause = true;
    }

    public SubscriptionMessage(){
        this.type = MsgType.SYNC;
    }

    public MsgType getType() { return type; }

    public FlowGroupInfo getFgi() { return fgi; }

    public double getRate() { return rate; }

    public boolean isPause() { return pause; }

}
