package gaiasim.gaiaagent;

// Stores subscription information

public class SubscriptionInfo {
    String id;
    FlowGroupInfo fgi;
    double rate;

    public SubscriptionInfo(String id, FlowGroupInfo fgi, double rate) {
        this.id = id;
        this.fgi = fgi;
        this.rate = rate;
    }

    public String getId() {
        return id;
    }

    public FlowGroupInfo getFgi() {
        return fgi;
    }

    public double getRate() {
        return rate;
    }
}
