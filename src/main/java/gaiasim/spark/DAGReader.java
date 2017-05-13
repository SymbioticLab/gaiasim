package gaiasim.spark;

import gaiasim.network.Coflow;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;


// Currently DAGReader maps coflow to stages (n+1),
// but actually should map to shuffles (n), which saves an empty coflow.

// Target PseudoCode:
// For each job{
//        Create HashMap h1 <stage,List_of_Locations>
//        Create HashMap h2 <End_Stage,CoFlow>
//        Read metadata < 3 1 0 >
//        For each stage (3 stages){
//        Read stage < Map2 1 n1>
//        Store HashMap h1 <stage,List_of_Locations>
//        }
//        Read number of shuffles <2>
//        For each shuffle (2 shuffles){
//        Read shuffle <Map2 Reducer2 5>
//        Check if (the End_Stage (Reducer2) is already in h2.) {
//        If so, add shuffle to the Coflow h2.get(Reducer2)
//        If not, add Coflow <Map2 Reducer2 5> to h2
//        }
//        }
//        }


public class DAGReader {

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

            HashMap<String, Coflow> coflow_map = new HashMap<String, Coflow>();
            // Get stage metadata
            // Previously we create a coflow per stage.
            // Now we only store the location tag of stages (temporarily), and refer to them later.
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
                coflow_map.put(stage_id, new Coflow(job_id + ':' + stage_id, task_locs));
            }

            // Determine coflow dependencies
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
                data_size = Math.max(1, data_size) * 8; // * 8 to convert to megabits

                Coflow child = coflow_map.get(src_task);
                Coflow parent = coflow_map.get(dst_task);
                child.parent_coflows.add(parent);
                parent.child_coflows.add(child);
                child.volume_for_parent.put(parent.getId(), (double)data_size);
                System.out.println("DAGReader: putting data_size " + data_size + " into " + parent.getId());
            }

            jobs.put(job_id, new Job(job_id, arrival_time, coflow_map));
        }

        br.close();
        return jobs;
    }
}
