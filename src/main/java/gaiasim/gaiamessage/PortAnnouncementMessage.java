package gaiasim.gaiamessage;

// new PA message.
// when SA receives a socket connection from master, it will create many persistent connections
// and send the port number of these connections for the master to setup SDN rules.

// Instead of sending a pack of PA_Messages, we want to batch the information in one message.

//Structure:
//SA (this)  * 1
//        RA (dst)   * (n):
//        for each RA, we have k {Path_id -> Port} tuples

// in total we have (n-1) * k tuples, i.e. (n) * k matching rules (port) for the SDN...
// Total number of rules for SDN is (n) * k * (n) * (avg switches per conn)

public class PortAnnouncementMessage extends AgentMessage{

    private String fromID;
    private int [] raList;
    private int [][] ra_path_port_Mapping;

    public PortAnnouncementMessage(String fromID, int[] raList, int[][] ra_path_port_mapping) {
        this.type = Type.PORT_ANNOUNCEMENT;

        this.fromID = fromID;
        this.raList = raList;
        this.ra_path_port_Mapping = ra_path_port_mapping;

    }

    public String getFromID() {     return fromID;    }

    public int[] getRaList() {     return raList;    }

    public int[][] getRa_path_port_Mapping() {       return ra_path_port_Mapping;    }

}
