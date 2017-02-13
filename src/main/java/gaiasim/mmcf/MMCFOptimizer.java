package gaiasim.mmcf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import gaiasim.network.Coflow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;

public class MMCFOptimizer {
    public static void glpk_optimize(Coflow coflow, NetGraph net_graph, Link[][] links) throws Exception {
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
            nid++;
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

        // Solve the LP
        String out_file_name = path_root + "/" + coflow.id_ + ".out";
        String command = "glpsol -m " + mod_file_name + " -d " + dat_file_name + " -o " + out_file_name;

        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Read the output
        double completion_time = 0.0;
        boolean missing_pieces = false;
        FileReader fr = new FileReader(out_file_name);
        BufferedReader br = new BufferedReader(fr);
        String line;
        String fs = "";
        String fe = "";
        while ((line = br.readLine()) != null) {
            if (line.contains("Objective")) {
                double alpha = Double.parseDouble(line.split("\\s+")[3]);
                if (alpha < 0.00001) {
                    System.out.println("Given coflow cannot be allocated on current network");
                    System.exit(1);
                }
                else {
                    completion_time = 1.0 / alpha;
                }
            }
            else if (line.contains("f[f") && !line.contains("NL")) {
                String[] splits = line.split("\\s+");
                String fsplits[] = splits[2].substring(3).split(",");
                int fi = Integer.parseInt(fsplits[0]);
                fs = int_to_nid.get(Integer.parseInt(fsplits[1]));
                fe = int_to_nid.get(Integer.parseInt(fsplits[2].split("]")[0]));

                try {
                    // Quick hack to round to nearest 2 decimal places
                    double bw = Math.round(Double.parseDouble(splits[3]) * 100.0) / 100.0;
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        // TODO: Insert to flow_kv
                    }
                    missing_pieces = false;
                }
                catch (Exception e) {
                    missing_pieces = true;
                }
            }
            else if (!line.contains("f[f") && !line.contains("NL") && missing_pieces) {
                String[] splits = line.split("\\s+");
                try {
                    double bw = Math.round(Math.abs(Double.parseDouble(splits[1]) * 100.0) / 100.0);
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        // TODO: Insert to flow_kv
                    }
                    missing_pieces = false;
                }
                catch (Exception e) {
                    missing_pieces = true;
                }
            }
            else if (line.contains("alpha")) {
                missing_pieces = false;
            }
        }
        br.close();
        System.out.println("Completion time = " + completion_time);
        System.exit(1);
    }
}
