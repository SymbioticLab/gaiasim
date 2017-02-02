package gaiasim.spark;

import gaiasim.network.NetGraph;

import java.io.BufferedReader;
import java.io.FileReader;

public class DAGReader {

    public static void read_trace(String filepath, NetGraph net_graph) throws java.io.IOException {
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
            int job_id = Integer.parseInt(splits[1]);
            int arrival_time = Integer.parseInt(splits[2]);

            System.out.println("Job with " + num_stages + " stages, ID " + job_id + " arrival time " + arrival_time);

            // Get stage metadata
            for (int i = 0; i < num_stages; i++) {
                line = br.readLine();
                splits = line.split(" ");
                String stage_id = splits[0];
                int num_tasks = Integer.parseInt(splits[1]);
                
                // TODO(jack): Parse/generate task locations.
                System.out.println("Stage " + stage_id + " with " + num_tasks + " tasks");
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
            }
        }

        br.close();
    }
}
