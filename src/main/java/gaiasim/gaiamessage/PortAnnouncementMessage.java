package gaiasim.gaiamessage;

// new PA message.
// when SA receives a socket connection from master, it will create many persistent connections
// and send the port number of these connections for the master to setup SDN rules.

// Instead of sending a pack of PA_Messages, we want to batch the information in one message.

//Structure:
//SA (this)  * 1
//        RA (dst)   * (n):
//        for each RA, we have k {Path_id -> Port} tuples (but for different RA we have different k)
// so we can't efficiently batch messages for different RAs. It's also fine, because this message is sent only once.

// in total we have (n-1) * k tuples, i.e. (n) * k matching rules (port) for the SDN...
// Total number of rules for SDN is (n) * k * (n) * (avg switches per conn)

public class PortAnnouncementMessage extends AgentMessage{

    private String fromID;
    private int raID;
    private int [] pathID_to_Port;

    public PortAnnouncementMessage(String fromID, int raID, int[] pathID_to_Port) {
        this.fromID = fromID;
        this.raID = raID;
        this.pathID_to_Port = pathID_to_Port;
    }

    public String getFromID() {     return fromID;    }

    public int getRaID() { return raID; }

    public int getPortfromPath(int pathID) { return pathID_to_Port[pathID]; }

}
