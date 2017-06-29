package gaiasim.mmcf;

import gaiasim.network.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class MaxFlowOptimizer {
    public static MaxFlowOutput glpk_optimize(Coflow coflow, NetGraph net_graph, SubscribedLink[][] links) throws Exception {
        long lastTime = System.nanoTime();
        String path_root = "/tmp";
        String mod_file_name = path_root + "/MaxFlow.mod";
        StringBuilder dat_string = new StringBuilder();
        dat_string.append("data;\n\n");

        dat_string.append("set N:=");
        for (int i = 1; i <= net_graph.nodes_.size(); i++) {
            dat_string.append(" " + i);
        }
        dat_string.append(";\n");

        ArrayList<Integer> flow_int_id_list = new ArrayList<>();
        HashMap<Integer, String> flow_int_id_to_id = new HashMap<>();
        System.out.println("Coflow " + coflow.id_ + " has flows: ");
        for (String k : coflow.flows_.keySet()) {
            Flow f = coflow.flows_.get(k);
            System.out.println("  " + k + ": " + f.src_loc_ + "-" + f.dst_loc_);
            int int_id = coflow.flows_.get(k).int_id_;
            flow_int_id_list.add(int_id);
            flow_int_id_to_id.put(int_id, k);
        }
        Collections.sort(flow_int_id_list);

        dat_string.append("set F:=");
        for (int fid : flow_int_id_list) {
            dat_string.append(" f" + fid);
        }
        dat_string.append(";\n\n");

        dat_string.append("param b:\n");
        for (int i = 1; i <= net_graph.nodes_.size(); i++) {
            dat_string.append(i + " ");
        }
        dat_string.append(":=\n");
        for (int i = 1; i <= net_graph.nodes_.size(); i++) {
            dat_string.append(i + " ");
            for (int j = 1; j <= net_graph.nodes_.size(); j++) {
                if (i == j || links[i][j] == null) {
                    dat_string.append(" 0.000");
                } else {
                    dat_string.append(String.format(" %.3f", links[i][j].remaining_bw()));
                }
            }
            dat_string.append("\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fs:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + coflow.flows_.get(flow_id).src_loc_ + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("param fe:=\n");
        for (int fid : flow_int_id_list) {
            String flow_id = flow_int_id_to_id.get(fid);
            dat_string.append(" f" + fid + " " + coflow.flows_.get(flow_id).dst_loc_ + "\n");
        }
        dat_string.append(";\n\n");

        dat_string.append("end;\n");

        String dat_file_name = path_root + "/" + coflow.id_ + ".dat";

        try {
            PrintWriter writer = new PrintWriter(dat_file_name, "UTF-8");
            writer.println(dat_string.toString());
            writer.close();
        } catch (java.io.IOException e) {
            System.out.println("ERROR: Failed to write to file " + dat_file_name);
            System.exit(1);
        }

        // Solve the LP
        String out_file_name = path_root + "/" + coflow.id_ + ".out";
        String command = "glpsol -m " + mod_file_name + " -d " + dat_file_name + " -o " + out_file_name;

        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Read the output
        MaxFlowOutput mf_out = new MaxFlowOutput();
        boolean missing_pieces = false;
        FileReader fr = new FileReader(out_file_name);
        BufferedReader br = new BufferedReader(fr);
        String line;
        String fs = "";
        String fe = "";
        int fi_int = -1;
        while ((line = br.readLine()) != null) {
            if (line.contains("Objective")) {
                double mu = Double.parseDouble(line.split("\\s+")[3]);
                if (mu < 0.00001) {
                    System.out.println("Given flows cannot be allocated on current network");
                    mf_out.max_util = -1.0;
                    return mf_out;
                } else {
                    mf_out.max_util = mu;
                }
            } else if (line.contains("f[f") && !line.contains("NL")) {
                String[] splits = line.split("\\s+");
                String fsplits[] = splits[2].substring(3).split(",");
                fi_int = Integer.parseInt(fsplits[0]);
                fs = fsplits[1];
                fe = fsplits[2].split("]")[0];
                try {
                    // Quick hack to round to nearest 2 decimal places
                    double bw = Math.round(Double.parseDouble(splits[4]) * 100.0) / 100.0;
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        if (mf_out.flow_link_bw_map_.get(fi_int) == null) {
                            mf_out.flow_link_bw_map_.put(fi_int, new ArrayList<>());
                        }
                        mf_out.flow_link_bw_map_.get(fi_int).add(new Link(fs, fe, bw));
                    }
                    missing_pieces = false;
                } catch (Exception e) {
                    missing_pieces = true;
                }
            } else if (!line.contains("f[f") && !line.contains("NL") && missing_pieces) {
                String[] splits = line.split("\\s+");
                try {
                    double bw = Math.round(Math.abs(Double.parseDouble(splits[2]) * 100.0) / 100.0);
                    if (bw >= 0.01 && !fs.equals(fe)) {
                        // At this point the flow id should be registered in the map
                        mf_out.flow_link_bw_map_.get(fi_int).add(new Link(fs, fe, bw));
                    }
                    missing_pieces = false;
                } catch (Exception e) {
                    missing_pieces = true;
                }
            } else if (line.contains("alpha")) {
                missing_pieces = false;
            }
        }
        br.close();

        long curTime = System.nanoTime();
        System.out.println("Calling LP (including File I/O) cost (ns) : " + (curTime - lastTime));
        return mf_out;
    }

    public static class MaxFlowOutput {
        public double max_util = 0.0;
        public HashMap<Integer, ArrayList<Link>> flow_link_bw_map_
                = new HashMap<>();
    }
}
