package gaiasim.mmcf;

import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.NetGraph;

public class MMCFOptimizer {
    public static void glpk_optimize(Coflow coflow, NetGraph net_graph) {
        String path_root = "/tmp";
        String mod_file_name = path_root + "/MinCCT.mod";
        StringBuilder dat_string = new StringBuilder();
        dat_string.append("data;\n\n");

        HashMap<String, int> int_to_nid = new HashMap<String, int>();
        HashMap<int, String> nid_to_int = new HashMap<int, String>();

        int nid = 0;
        for (String n : net_graph.nodes_) {
            int_to_nid[nid] = n;
            nid_to_int[n] = nid;
        }
        
        dat_string.append("set N:=");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(" " + i);
        }
        dat_string.append(";\n");

        ArrayList<int> flow_id_list = new ArrayList<int>();
        for (String k : coflow.flows_.keySet()) {
            flow_id_list.append(coflow.flows_.get(k).int_id_);
        }
        Collections.sort(flow_id_list);

        dat_string.append("set F:=");
        for (int fid : flow_id_list) {
            dat_string.append(" f" + fid); // NOTE: Original mmcf did fid-1
        }

    }
}
