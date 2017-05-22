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
// good news being that the array is sparse, so we may think of some way to compress it.



// This message corresponds to all PCs and all flows between one (SA -> RA) pair

import java.io.Serializable;

public class FlowUpdateMessage implements Serializable{

    String raID;

    int sizeOfFlowGroups;
    int sizeOfPaths; // number of paths

    String [] fgID;

    double [] remaingVolume; // the first time receiving this update, store it, then neglect future updates.

    double [][] rates; // rates[size]

    public FlowUpdateMessage(String raID, int sizeOfFlowGroups, int sizeOfPaths, String[] fgID, double[] remaingVolume, double[][] rates) {
        this.raID = raID;
        this.sizeOfFlowGroups = sizeOfFlowGroups;
        this.sizeOfPaths = sizeOfPaths;
        this.fgID = fgID;
        this.remaingVolume = remaingVolume;
        this.rates = rates;
    }

    public String getRaID() { return raID; }

    public int getSizeOfFlowGroups() {
        return sizeOfFlowGroups;
    }

    public int getSizeOfPaths() {
        return sizeOfPaths;
    }

    public String[] getFgID() {
        return fgID;
    }

    public double[] getRemaingVolume() {
        return remaingVolume;
    }

    public double getRemaingVolume(int i){
        return remaingVolume[i];
    }

    public double[][] getRates() {
        return rates;
    }

    public double getRate(int i, int j){
        return rates[i][j];
    }

}
