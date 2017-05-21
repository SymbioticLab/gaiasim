package gaiasim.gaiaagent;

// The abstract class for messages sent into Workers's event queue.


public abstract class WorkerMessage {
    public enum Type{
        SUBSCRIBE,
        UNSUBSCRIBE
    }

    protected Type type;

    public Type getType() { return type; }
}
