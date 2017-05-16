package gaiasim.gaiamessage;

// The base class of AgentMessage

import java.io.Serializable;

public abstract class AgentMessage implements Serializable {
    public enum Type{
        FLOW_STATUS,
        PORT_ANNOUNCEMENT
    }

    protected Type type;

    public Type getType() { return type; }

    // no need to define serializer etc. use objectOutputStream or SerializationUtils.serialize(yourObject);
}
