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
//          Read stage < Map2 1 n1>
//          Store HashMap h1 <stage,List_of_Locations>
//        }
//        Read number of shuffles <2>
//        For each shuffle (2 shuffles){
//          Read shuffle <Map2 Reducer2 5>
//          Check if (the End_Stage (Reducer2) is already in h2.) {
//              If so, add shuffle to the Coflow h2.get(Reducer2)
//              If not, add Coflow <Map2 Reducer2 5> to h2
//          }
//        }
// }


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
        while ((line = br.readLine()) != null) { // for each job

            // Ignore comments
            if (line.charAt(0) == '#') {
                line = br.readLine();
            }

            // Get job metadata
            String[] splits = line.split(" ");
            int num_stages = Integer.parseInt(splits[0]);
            String job_id = splits[1];
            long arrival_time = Integer.parseInt(splits[2]) * Constants.MILLI_IN_SECOND;


            // store location of stages in this job.
            HashMap<String, String[]> location_map = new HashMap<String, String[]>();

            // Get stage metadata
            // store the location tag of stages (temporarily), and refer to them later.
            for (int i = 0; i < num_stages; i++) {
                // for each stage, store the locations of tasks.
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

                // store location info
                location_map.put(stage_id, task_locs);

//                coflow_map.put(stage_id, new Coflow(job_id + ':' + stage_id, dst_locs));
            }

            // map coflows to their destination stage.
            HashMap<String, Coflow> coflow_map = new HashMap<String, Coflow>();

            // Map coflow and Determine coflow dependencies
            line = br.readLine();
            splits = line.split(" ");
            int num_shuffles = Integer.parseInt(splits[0]);
            for (int i = 0; i < num_shuffles; i++) {
                line = br.readLine(); // for each shuffle
                splits = line.split(" ");
                String src_stage = splits[0];
                String dst_stage = splits[1];

                // Int is enough (2 Peta-bit data)
                int data_size = Integer.parseInt(splits[2]);
                data_size = Math.max(1, data_size) * 8; // * 8 to convert to megabits

                // first check if is owned by an existing coflow
                if(coflow_map.containsKey(dst_stage)){
                    Coflow owning_coflow = coflow_map.get(dst_stage);
                    owning_coflow.addShuffle(src_stage , data_size);
                    // Then resolve dependencies.

                    //  coflow_map.put(stage_id, new Coflow(job_id + ':' + stage_id, dst_locs));
                }
                else {
                    Coflow new_coflow = new Coflow( job_id , dst_stage , location_map.get(dst_stage)); // init an empty coflow
                    new_coflow.addShuffle();

                    coflow_map.put(dst_stage,new_coflow);

                    // Then resolve dependencies.
                }



                Coflow child = coflow_map.get(src_stage);
                Coflow parent = coflow_map.get(dst_stage);
                child.parent_coflows.add(parent);
                parent.child_coflows.add(child);
                child.volume_for_parent.put(parent.getId(), (double)data_size);
//                System.out.println("DAGReader: putting data_size " + data_size + " into " + parent.getId());
            }

            jobs.put(job_id, new Job(job_id, arrival_time, coflow_map));
        }

        br.close();
        return jobs;
    }
}
