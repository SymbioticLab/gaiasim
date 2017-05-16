package gaiasim.gaiamessage;

// new PA message.
// when SA receives a socket connection from master, it will create many persistent connections
// and send the port number of these connections for the master to setup SDN rules.

// Instead of sending a pack of PAMessages, we want to batch the information in one message.

public class PortAnnouncemendMessage extends AgentMessage{

    private int sa_id;
    private int ra_id;
    private int path_id;
    private int port;

    public PortAnnouncemendMessage() {
        this.type = Type.PORT_ANNOUNCEMENT;
    }
}
