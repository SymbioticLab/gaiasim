package gaiasim.gaiaagent;

// Message for subscribe a FlowGroup to a persistent connection (worker).

public class CTRL_to_WorkerMsg {
    public enum  MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        CONNECT,
        SYNC // SYNC do not carry information
    }

    MsgType type;

    FlowGroupInfo fgi;
    double rate = 0.0;
    boolean pause = false;

    // subscribe or change rate.
    public CTRL_to_WorkerMsg(FlowGroupInfo fgi, double rate) {
        this.type = MsgType.SUBSCRIBE;
        this.fgi = fgi;
        this.rate = rate;
        this.pause = false;
    }

    // unsubscribe
    public CTRL_to_WorkerMsg(FlowGroupInfo fgi) {
        this.type = MsgType.UNSUBSCRIBE;
        this.fgi = fgi;
        this.rate = 0.0;
        this.pause = true;
    }

    public CTRL_to_WorkerMsg(int NULL){
        this.type = MsgType.CONNECT;
    }

    public CTRL_to_WorkerMsg(){
        this.type = MsgType.SYNC;
    }

    public MsgType getType() { return type; }

    public FlowGroupInfo getFgi() { return fgi; }

    public double getRate() { return rate; }

    public boolean isPause() { return pause; }

}
