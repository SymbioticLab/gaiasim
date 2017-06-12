package gaiasim.spark;

import gaiasim.network.Coflow_Old;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class DAGReader_old {

    public static HashMap<String, Job> read_trace(String filepath, NetGraph net_graph) throws java.io.IOException {
        HashMap<String, Job> jobs = new HashMap<String, Job>();

        // For now, use the same seed between runs.
        // Currently this is only used for assigning nodes to task
        // when a trace comes without such information.
        Random rnd = new Random(13);

        FileReader fr = new FileReader(filepath);
        BufferedReader br = new BufferedReader(fr);

        String line;
        while ((line = br.readLine()) != null) {

            // Ignore comments
            if (line.charAt(0) == '#') {
                line = br.readLine();
            }

            // Get job metadata
            String[] splits = line.split(" ");
            int num_coflows = Integer.parseInt(splits[0]);
            String job_id = splits[1];
            long arrival_time = Integer.parseInt(splits[2]) * Constants.MILLI_IN_SECOND;

            HashMap<String, Coflow_Old> Coflow_Old_map = new HashMap<String, Coflow_Old>();
            // Get stage metadata
            for (int i = 0; i < num_coflows; i++) {
                line = br.readLine();
                splits = line.split(" ");
                String stage_id = splits[0];
                int num_tasks = Integer.parseInt(splits[1]);

                String[] task_locs = new String[num_tasks];

                // If the case came with trace with information about
                // task placement, use that. Otherwise choose random
                // nodes.
                if (splits.length > 2) {
                    for (int j = 0; j < num_tasks; j++) {
                        task_locs[j] = net_graph.trace_id_to_node_id_.get(splits[2 + j]);
                    }
                }
                else {
                    ArrayList<String> tmp_nodes = net_graph.nodes_;
                    Collections.shuffle(tmp_nodes, rnd);
                    for (int j = 0; j < num_tasks; j++) {
                        task_locs[j] = tmp_nodes.get(j % tmp_nodes.size());
                    }
                }
                Coflow_Old_map.put(stage_id, new Coflow_Old(job_id + ':' + stage_id, task_locs));
            }

            // Determine Coflow_Old dependencies
            line = br.readLine();
            splits = line.split(" ");
            int num_edges = Integer.parseInt(splits[0]);
            for (int i = 0; i < num_edges; i++) {
                line = br.readLine();
                splits = line.split(" ");
                String src_task = splits[0];
                String dst_task = splits[1];

                // TODO(jack): Consider making this a long
                int data_size = Integer.parseInt(splits[2]);
                data_size = Math.max(1, data_size) * 8; // * 8 to conver to megabits

                Coflow_Old child = Coflow_Old_map.get(src_task);
                Coflow_Old parent = Coflow_Old_map.get(dst_task);
                child.parent_coflows.add(parent);
                parent.child_coflows.add(child);
                child.volume_for_parent.put(parent.getId(), (double)data_size);
            }

            jobs.put(job_id, new Job(job_id, arrival_time, Coflow_Old_map));
        }

        br.close();
        return jobs;
    }
}