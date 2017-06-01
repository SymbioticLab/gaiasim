package gaiasim.spark;

// Read DAG and returns a list of Jobs. (unsorted), HashMap <ID , Job>
// Target PseudoCode of extracting coflows from DAGs. In real world this problem is handled by YARN etc.
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
//              If so, add shuffle to the Coflow_Old h2.get(Reducer2)
//              If not, add Coflow_Old <Map2 Reducer2 5> to h2
//          }
//        }
// }

import com.google.common.collect.ArrayListMultimap;
import gaiasim.GaiaSim;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DAGReader_New {
    public static HashMap<String, Job> read_trace_new(String tracefile, NetGraph net_graph) throws IOException {
        HashMap<String, Job> jobs = new HashMap<String, Job>();

        // For now, use the same seed between runs.
        // Currently this is only used for assigning nodes to task
        // when a trace comes without such information.
        Random rnd = new Random(13);

        System.out.println("DAGReader: reading from " + tracefile);

        FileReader fr = new FileReader(tracefile);
        BufferedReader br = new BufferedReader(fr);

        String line;
        while ((line = br.readLine()) != null) { // for each job

            // Ignore comments
            if (line.charAt(0) == '#') {
                line = br.readLine();
            }

            // Get Job metadata
            String[] splits = line.split(" ");
            int num_stages = Integer.parseInt(splits[0]);
            String dag_id = splits[1];
            long arrival_time = Integer.parseInt(splits[2]) * Constants.MILLI_IN_SECOND;

            DependencyResolver dr = new DependencyResolver(dag_id);

            // store location of stages in this job.
            HashMap<String, String[]> locationMap = new HashMap<String, String[]>();

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
                else { // randomize task location assignment (not really needed)
                    ArrayList<String> tmp_nodes = net_graph.nodes_;
                    Collections.shuffle(tmp_nodes, rnd);
                    for (int j = 0; j < num_tasks; j++) {
                        task_locs[j] = tmp_nodes.get(j % tmp_nodes.size());
                    }
                }

                // store location info
                locationMap.put(stage_id, task_locs);

            }


            // create a buffer for constructing Coflows.
            // when finished reading this job, we can recover all Coflow information from it.
            // maps Coflow_id to FlowGroups.
            ArrayListMultimap<String , Flow> tmpCoflowList = ArrayListMultimap.create();

            ArrayList<Flow> tmpFlowList = new ArrayList<>();
            int flowIDCounter = 0;

            // Map coflow and Determine coflow dependencies
            line = br.readLine();
            splits = line.split(" ");
            int num_shuffles = Integer.parseInt(splits[0]);
            for (int i = 0; i < num_shuffles; i++) {
                line = br.readLine(); // for each shuffle
                splits = line.split(" ");
                String src_stage = splits[0];
                String dst_stage = splits[1];

                // Direct read Double data
                double data_size = Double.parseDouble(splits[2]) ;
                // Convert to megabits, then divide by FlowGroups
                int numberOfFlowGroups = locationMap.get(src_stage).length * locationMap.get(dst_stage).length;
                double divided_data_size = Math.max(1, data_size) * 8 / numberOfFlowGroups;

                // create FlowGroups and add to buffer.
                for( String srcLoc : locationMap.get(src_stage)){
                    for (String dstLoc : locationMap.get(dst_stage)){
                        // id - job_id:srcStage:dstStage:srcLoc-dstLoc // encoding task location info.
                        // src - srcLoc
                        // dst - dstLoc
                        // owningCoflowID - JobID:dstStage
                        // Volume - divided_data_size

                        Flow f = new Flow(dag_id + ':' + src_stage + ':' + dst_stage + ':' + srcLoc + '-' + dstLoc,
                                flowIDCounter , dag_id + ':' + dst_stage, srcLoc, dstLoc, divided_data_size );

                        flowIDCounter++; // We don't know how many coflows we have. So we use a counter per Job, as long as it is unique

                        if ( !srcLoc.equals(dstLoc) ) { // Ignore co-located Flow.
                            tmpFlowList.add(f);
                            tmpCoflowList.put(dag_id + ":" + dst_stage , f);
                        }
                        else {
                            System.out.println("Skipping Flow " + f.id_ + " because Src and Dst are same");
                        }

                    }
                }

                // Then update dependencies and the "root"
                // Note that after this operation "root" is not Coflow_root, we need to trim() the DAG.
                dr.updateDependency( src_stage , dst_stage);

            } // end of current DAG
            // flush the Coflows from the buffer to dag, this also creates the coflow instances.
            dr.addCoflows(tmpCoflowList);
            dr.updateRoot();

            // create the actual job using the dependency information from dr
            Job job = new Job(dag_id , arrival_time);
            job.coflows_.putAll(dr.coflowList);

            // read the child/parent info from coflow, and generate the start/end coflow list.
            job.start_coflows_.addAll(dr.getRootCoflows());
            // Though end_coflows are not used, we still create them.
            for (Map.Entry<String,Coflow> entry : job.coflows_.entrySet()) {
                Coflow cf = entry.getValue();
                if (cf.child_coflows.size() == 0) {
                    job.end_coflows_.add(cf);
                }
            }

            jobs.put( dag_id , job);

        } // end of trace.txt
        br.close();

        // return without sorting.
        return jobs;
    }
}
