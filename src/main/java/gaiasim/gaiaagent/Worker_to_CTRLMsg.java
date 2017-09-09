package gaiasim.gaiaagent;

import gaiasim.gaiaprotos.GaiaMessageProtos;

public class Worker_to_CTRLMsg {
    public enum  MsgType {
        FLOWSTATUS,
        LINKSTATUS
    }

    public MsgType type;

    public GaiaMessageProtos.FlowStatusReport statusReport;

    public Worker_to_CTRLMsg(GaiaMessageProtos.FlowStatusReport statusReport){
        this.statusReport = statusReport;
        this.type = MsgType.FLOWSTATUS;
    }


}
