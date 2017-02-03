package gaiasim.spark;

import gaiasim.network.NetGraph;
import gaiasim.spark.Stage;
import gaiasim.spark.Job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

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
            int num_stages = Integer.parseInt(splits[0]);
            String job_id = splits[1];
            int arrival_time = Integer.parseInt(splits[2]);

            System.out.println("Job with " + num_stages + " stages, ID " + job_id + " arrival time " + arrival_time);
            HashMap<String, Stage> stage_map = new HashMap<String, Stage>();
            // Get stage metadata
            for (int i = 0; i < num_stages; i++) {
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
                        task_locs[j] = splits[2 + j];
                    }
                }
                else {
                    ArrayList<String> tmp_nodes = net_graph.nodes_;
                    Collections.shuffle(tmp_nodes, rnd);
                    for (int j = 0; j < num_tasks; j++) {
                        System.out.println("Adding " + tmp_nodes.get(j % tmp_nodes.size()));
                        task_locs[j] = tmp_nodes.get(j % tmp_nodes.size());
                    }
                }
                System.out.println("Stage " + stage_id + " with " + num_tasks + " tasks");
                stage_map.put(stage_id, new Stage(job_id + ':' + stage_id, task_locs));
            }

            // Determine stage dependencies
            line = br.readLine();
            splits = line.split(" ");
            int num_edges = Integer.parseInt(splits[0]);
            for (int i = 0; i < num_edges; i++) {
                line = br.readLine();
                splits = line.split(" ");
                String src_stage = splits[0];
                String dst_stage = splits[1];

                // TODO(jack): Consider making this a long
                int data_size = Integer.parseInt(splits[2]); 
                data_size = Math.max(1, data_size) * 8; // * 8 to conver to megabits
                System.out.println("Dependency from " + src_stage + " to " + dst_stage + " transferring " + data_size);

                Stage child = stage_map.get(src_stage);
                Stage parent = stage_map.get(dst_stage);
                child.parent_stages_.add(parent);
                parent.child_stages_.add(child);
                child.volume = (double)data_size;
            }
 
            jobs.put(job_id, new Job(job_id, arrival_time, stage_map));
        }

        br.close();
        return jobs;
    }
}
