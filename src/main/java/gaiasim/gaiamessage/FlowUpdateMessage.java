package gaiasim.gaiamessage;

// This is FLOW_UPDATE message sent from the master to Sending Agents.
// We want to batch the flow rates. Essientially combing FLOW_START, FLOW_UPDATE, SUBFLOW_INFO into one message.

// including remaining volume of the FlowGroup.. so that SA can stop after completion.
// including the subflow information (path_id -> rate) , since SA already has the {path_id -> persistenConn mapping}

// FlowGroups may represent different clients but may also share the same path.
// we want to batch so that for each SA we only send one FLOW_UPDATE (not feasible, because different RA has different paths)

// let's say we have k destinations (NY to LA, NY to HK, etc.)
//      each (from NY to LA) containing m FlowGroups (representing different shuffles from different clients) ,
//              each have n sub-flow (different paths with different rates.)
// Altogether we need an (k * m * n) Array of {pathid -> rate}.

// The output of the LP is an #SA * k * m * n Array.
// good news being that the array is sparse, so we can think of some way to compress it.



public class FlowUpdateMessage {

    String fgID;
    String raID;

    int size; // number of paths
    double remaingVolume; // the first time receiving this update, store it, then neglect future updates.

    double [] rates; // rates[size]








}
