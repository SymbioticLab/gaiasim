package gaiasim.network;

public class Flow {
    public String id;
    public float volume;
    public float transmitted;
    public float rate;

    public Flow(String id_, float volume_, float rate_) {
        id = id_;
        volume = volume_;
        rate = rate_;
        transmitted = (float)0.0;
    }
};
