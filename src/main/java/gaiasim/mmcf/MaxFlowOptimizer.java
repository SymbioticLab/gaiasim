package gaiasim.mmcf;

import gaiasim.network.Coflow;
import gaiasim.network.Link;
import gaiasim.network.NetGraph;
import gaiasim.network.SubscribedLink;
import gaiasim.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

@SuppressWarnings("Duplicates")
public class MaxFlowOptimizer {

    static NetGraph netGraph;
    private static ArrayList<Integer> flow_int_id_list;

    public static MaxFlowOutput glpk_optimize(Coflow coflow, NetGraph net_graph, SubscribedLink[][] links) throws Exception {
        netGraph = net_graph;
        long lastTime = System.currentTimeMillis();
        String path_root = "/tmp";
        String mod_file_name = path_root + "/MaxFlow.mod";
        StringBuilder dat_string = new StringBuilder();
        dat_string.append("data;\n\n");

        dat_string.append("set N:=");
        for (int i = 1; i <= net_graph.nodes_.size(); i++) {
            dat_string.append(" " + i);
        }
        dat_string.append(";\n");

        flow_int_id_list = new ArrayList<>();
        HashMap<Integer, String> flow_int_id_to_id = new HashMap<>();
//        System.out.println("Coflow " + coflow.id_ + " has flows: ");
        for (String k : coflow.flows_.keySet()) {
//            Flow f = coflow.flows_.get(k);
//            System.out.println("  " + k + ": " + f.src_loc_ + "-" + f.dst_loc_);
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
        } catch (IOException e) {
            System.out.println("ERROR: Failed to write to file " + dat_file_name);
            System.exit(1);
        }

        // Solve the LP
        String out_file_name = path_root + "/" + coflow.id_ + ".out";
//        MaxFlowOutput mf_out = solveLP_Old(mod_file_name, dat_file_name, out_file_name);
        MaxFlowOutput mf_out = solveLP_New(mod_file_name, dat_file_name, out_file_name);

        long curTime = System.currentTimeMillis();
        System.out.println("Calling LP (including File I/O) cost (ms) : " + (curTime - lastTime));
        return mf_out;
    }

    /*private static MaxFlowOutput solveLP_Old(String mod_file_name, String dat_file_name, String out_file_name) throws IOException {

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
            line = line.trim();

            // concatenate the lines if the first line has "[", and ends with "]"
            if (line.contains("[") && (line.substring(line.length() - 1).equals("]"))) {
                line = line + br.readLine();
            }

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
                String fsplits[] = splits[1].substring(3).split(",");
                fi_int = Integer.parseInt(fsplits[0]);
                fs = fsplits[1];
                fe = fsplits[2].split("]")[0];
                try {
                    // Quick hack to round to nearest 2 decimal places
                    double bw = Math.round(Double.parseDouble(splits[3]) * 100.0) / 100.0;
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
                    double bw = Math.round(Math.abs(Double.parseDouble(splits[1]) * 100.0) / 100.0);
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

        return mf_out;
    }
*/

    private static MaxFlowOutput solveLP_New(String mod_file_name, String dat_file_name, String out_file_name) throws IOException {
        String command = "glpsol -m " + mod_file_name + " -d " + dat_file_name + " -w " + out_file_name;

        long startTime = System.currentTimeMillis();
        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
//            System.exit(1);

            // TODO failure handling
        }

        startTime = System.currentTimeMillis() - startTime;
        System.out.println("LP time: " + startTime);

        // Read the output
        MaxFlowOutput mf_out = parsePlainTextOutput(out_file_name);

        return mf_out;
    }

    private static MaxFlowOutput parsePlainTextOutput(String out_file_name) throws IOException {
        MaxFlowOutput mf_out = new MaxFlowOutput();
        FileReader fr = new FileReader(out_file_name);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int m = 0;
        int n = 0;

        // read the output header
        if((line = br.readLine()) != null){
            String[] splits = line.split(" ");
            m = Integer.parseInt(splits[0]);
            n = Integer.parseInt(splits[1]);
        }
        else {
            System.err.println("Error: empty LP output");
            return null;
        }

        // 1.  check if it is satisfied

        // read the output header
        if((line = br.readLine()) != null){
            String[] splits = line.split(" ");
            int prim_stat = Integer.parseInt(splits[0]);
            int dual_stat = Integer.parseInt(splits[1]);
            double mu = Double.parseDouble(splits[2]);
            if (mu < Constants.VALID_CCT_THR || prim_stat != 2 || dual_stat !=2 ){
                System.out.println("Given coflow cannot be allocated on current network");
                mf_out.max_flow = -1.0;
                return mf_out;
            } else {
                mf_out.max_flow = mu;
            }
        }
        else {
            System.err.println("Error: wrong LP output");
            return null;
        }

        // 2. skip the row results

        for (int i = 0 ; i < m ;i ++ ){
            br.readLine();
        }

        // 3. read the flow rates

        // for in {startNode, endNode, flowID}
        for (int fs = 1; fs <= netGraph.nodes_.size(); fs++){
            for (int fe = 1; fe <= netGraph.nodes_.size(); fe++){
                for (int fi_int : flow_int_id_list) {

                    line = br.readLine();  // TODO: what if for a flow the rate along the edges are different? -> LP failed, handle in make_paths

                    String [] splits = line.split(" ");
                    // Quick hack to round to nearest 2 decimal places
                    double rate = Math.round( Double.parseDouble(splits[1]) * 100) / 100.0;

                    if (rate < Constants.FLOW_RATE_THR){
                        continue;
                    }

//                    if (rate < Constants.FLOW_RATE_THR){
//                        System.err.println("Flow rate too low, cancel this flow");
//                        rate = 0;
//                    }

                    if (mf_out.flow_link_bw_map_.get(fi_int) == null) {
                        mf_out.flow_link_bw_map_.put(fi_int, new ArrayList<>());
                    }
                    mf_out.flow_link_bw_map_.get(fi_int).add(new Link(String.valueOf(fs), String.valueOf(fe), rate));

                }
            }
        }

        br.close();

        return mf_out;
    }

    public static class MaxFlowOutput {
        public double max_flow = 0.0;
        public HashMap<Integer, ArrayList<Link>> flow_link_bw_map_
                = new HashMap<>();
    }
}
