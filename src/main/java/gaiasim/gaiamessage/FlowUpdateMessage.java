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

// ver 1.0

// ver 2.0 FUM: use HashMap rather than arrays.
// essientially we want to store, for each SA (i.e. each FUM):
//  1.  (RA, Paths , FGID) -> rate
//  2.  (RA, FGID) -> volume
//  3.  (RA) -> FGID[]
//  4.  RA[]


import gaiasim.network.FlowGroup_Old;
import gaiasim.network.NetGraph;
import gaiasim.network.Pathway;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class FlowUpdateMessage implements Serializable{

    public class FlowGroupEntry implements Serializable{
        public double remainingVolume;
        public HashMap< Integer , Double> pathToRate;

        public FlowGroupEntry(FlowGroup_Old fgo, NetGraph ng) {
            this.remainingVolume = fgo.remaining_volume();
            this.pathToRate = new HashMap<>();
            for ( Pathway p : fgo.paths){
                pathToRate.put( ng.get_path_id(p) , p.getBandwidth() );
            }
        }
    }

    // raID -> fgID -> FGE  , so if we want to get rate: raID -> fgID -> pathID -> rate
    HashMap<String , HashMap<String , FlowGroupEntry>> content;

    /**
    * @param fgos a collection of FlowGroup_Old objects whose Src_location
    *             is the same as the intended recipient of this FUM.
    * @param ng   NetGraph for us to parse the information in fgos.
    */
    public FlowUpdateMessage(Collection<FlowGroup_Old> fgos, NetGraph ng, String saID){
        content = new HashMap<>();
        for(FlowGroup_Old fgo : fgos){ // for each FGO, we create an FlowGroupEntry
            assert (saID.equals(fgo.getSrc_loc()));
            String raID = fgo.getDst_loc();
            String fgoID = fgo.getId();
            FlowGroupEntry fge = new FlowGroupEntry(fgo , ng);

            // we may need to create the inner hashMap, so first check
            if(content.containsKey(raID)){
                content.get(raID).put(fgoID , fge);
            }
            else { // no inner hashmap
                HashMap<String , FlowGroupEntry> tmpMap = new HashMap<>();
                tmpMap.put(fgoID, fge);
                content.put(raID , tmpMap);
            }
        } // end for each FGO.
    }

    public HashMap<String, HashMap<String, FlowGroupEntry>> getContent() { return content; }

    @Override
    public String toString() {
        String ret = "FUM: ";
        for (Map.Entry<String, HashMap<String, FlowGroupEntry>> entry : content.entrySet()){
            for( Map.Entry<String, FlowGroupEntry> e : entry.getValue().entrySet()){
                ret += e.getKey() + "  ";
                for( Map.Entry<Integer , Double> rate : e.getValue().pathToRate.entrySet()){
                    ret += rate.getKey() + ":" + rate.getValue() + " ";
                }
            }
        }

        return ret;
    }

/*

    // Below is OLD ver 1.0 API
    public FlowUpdateMessage(String raID, int sizeOfFlowGroups, int sizeOfPaths, String[] fgID, double[] remaingVolume, double[][] rates) {
        this.raID = raID;
        this.sizeOfFlowGroups = sizeOfFlowGroups;
        this.sizeOfPaths = sizeOfPaths;
        this.fgID = fgID;
        this.remaingVolume = remaingVolume;
        this.rates = rates;
    }

    public double getRate(int i, int j){
        return rates[i][j];
    }


    String raID;

    int sizeOfFlowGroups;

    int sizeOfPaths; // number of paths

    String [] fgID;

    double [] remaingVolume; // the first time receiving this update, store it, then neglect future updates.

    double [][] rates; // rates[size]

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
*/
}
