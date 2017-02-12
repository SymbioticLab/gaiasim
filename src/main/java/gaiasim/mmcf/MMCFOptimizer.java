package gaiasim.mmcf;

import java.io.PrintWriter;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;

public class MMCFOptimizer {
    public static void glpk_optimize(Coflow coflow, NetGraph net_graph, Link[][] links) {
        String path_root = "/tmp";
        String mod_file_name = path_root + "/MinCCT.mod";
        StringBuilder dat_string = new StringBuilder();
        dat_string.append("data;\n\n");

        HashMap<String, Integer> nid_to_int = new HashMap<String, Integer>();
        HashMap<Integer, String> int_to_nid = new HashMap<Integer, String>();

        int nid = 0;
        for (String n : net_graph.nodes_) {
            int_to_nid.put(nid, n);
            nid_to_int.put(n, nid);
        }
        
        dat_string.append("set N:=");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(" " + i);
        }
        dat_string.append(";\n");

        ArrayList<Integer> flow_int_id_list = new ArrayList<Integer>();
        HashMap<Integer, String> flow_int_id_to_id = new HashMap<Integer, String>();
        for (String k : coflow.flows_.keySet()) {
            int int_id = coflow.flows_.get(k).int_id_;
            flow_int_id_list.add(int_id);
            flow_int_id_to_id.put(int_id, k);
        }
        Collections.sort(flow_int_id_list);

        dat_string.append("set F:=");
        for (int fid : flow_int_id_list) {
            dat_string.append(" f" + fid); // NOTE: Original mmcf did fid-1
        }
        dat_string.append(";\n\n");
        
        dat_string.append("param b:\n");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(i + " " );
        }
        dat_string.append(":=\n");
        for (int i = 0; i < net_graph.nodes_.size(); i++) {
            dat_string.append(i + " ");
            for (int j = 0; j < net_graph.nodes_.size(); j++) {
                if (i == j || links[i+1][j+1] == null) {
                    dat_string.append(" 0.000");
                }
                else {
                    dat_string.append(String.format(" %.3f", links[i+1][j+1].remaining_bw()));
                }
            }
            dat_string.append("\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fs:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + nid_to_int.get(coflow.flows_.get(flow_id).src_loc_) + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fe:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + nid_to_int.get(coflow.flows_.get(flow_id).dst_loc_) + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fv:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(String.format(" f%d %.3f\n", fid, coflow.flows_.get(flow_id).remaining_volume()));
        }
        dat_string.append(";\n\n");

        dat_string.append("end;\n");

        String dat_file_name = path_root + "/" + coflow.id_ + ".dat";

        try {
            PrintWriter writer = new PrintWriter(dat_file_name, "UTF-8");
            writer.println(dat_string.toString());
            writer.close();
        }
        catch (java.io.IOException e) {
            System.out.println("ERROR: Failed to write to file " + dat_file_name);
            System.exit(1);
        }
    }
}
