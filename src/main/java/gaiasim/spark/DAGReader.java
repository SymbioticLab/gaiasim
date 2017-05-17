package gaiasim.spark;

//import gaiasim.network.Coflow_Old;

import com.google.common.collect.ArrayListMultimap;
import gaiasim.gaiamaster.Coflow;
import gaiasim.gaiamaster.FlowGroup;
import gaiasim.network.NetGraph;
import gaiasim.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

// This is the DAGReader. (essientially DAG Reducer)
// it reads the trace.txt and put Job(DAG) into an event queue, according to the time of arrival.
// We implement it as a Runnable.

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

public class DAGReader implements Runnable{

    String tracefile;
    NetGraph netGraph;
    LinkedBlockingQueue<YARNMessages> yarnEventQueue;

    public DAGReader(String tracefile, NetGraph netGraph, LinkedBlockingQueue<YARNMessages> yarnEventQueue) {
        this.tracefile = tracefile;
        this.netGraph = netGraph;
        this.yarnEventQueue = yarnEventQueue;

        System.out.println("YARN: Initing DAGReader.");



    }


    @Override
    public void run() {

    }

    public static ArrayList<DAG> getListofDAGs( String filepath, NetGraph net_graph ) throws IOException {
        ArrayList<DAG> dagList = new ArrayList<DAG>();

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

            // Get DAG metadata
            String[] splits = line.split(" ");
            int num_stages = Integer.parseInt(splits[0]);
            String dag_id = splits[1];
            long arrival_time = Integer.parseInt(splits[2]) * Constants.MILLI_IN_SECOND;

            DAG dag = new DAG(dag_id , arrival_time);

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
                else {
                    ArrayList<String> tmp_nodes = net_graph.nodes_;
                    Collections.shuffle(tmp_nodes, rnd);
                    for (int j = 0; j < num_tasks; j++) {
                        task_locs[j] = tmp_nodes.get(j % tmp_nodes.size());
                    }
                }

                // store location info
                locationMap.put(stage_id, task_locs);

//                coflow_map.put(stage_id, new Coflow_Old(job_id + ':' + stage_id, dst_locs));
            }

            // map coflows to their destination stage.
//            HashMap<String, Coflow> coflow_map = new HashMap<String, Coflow>();

            // create a buffer for constructing Coflows.
            // when finished reading this job, we can recover all Coflow information from it.
            // maps FlowGroup to the Coflow_id.
            ArrayListMultimap<String , FlowGroup> tmpCoflowList = ArrayListMultimap.create();

            // Map coflow and Determine coflow dependencies
            line = br.readLine();
            splits = line.split(" ");
            int num_shuffles = Integer.parseInt(splits[0]);
            for (int i = 0; i < num_shuffles; i++) {
                line = br.readLine(); // for each shuffle
                splits = line.split(" ");
                String src_stage = splits[0];
                String dst_stage = splits[1];

                // Direct read Double data TODO: verify this works
                double data_size = Double.parseDouble(splits[2]);
                // Convert to megabits, then divide by FlowGroups
                int numberOfFlowGroups = locationMap.get(src_stage).length * locationMap.get(dst_stage).length;
                double divided_data_size = Math.max(1, data_size) * 8 / numberOfFlowGroups;

                // create FlowGroups and add to buffer.
                for( String srcLoc : locationMap.get(src_stage)){
                    for (String dstLoc : locationMap.get(dst_stage)){
                        // id - job_id:src:dst
                        // src - srcLoc
                        // dst - dstLoc
                        // owningCoflowID - dstStage
                        // Volume - divided_data_size
                        FlowGroup fg = new FlowGroup(dag_id + ':' + src_stage + ':' + dst_stage ,
                            srcLoc, dstLoc , dst_stage , divided_data_size);
                        tmpCoflowList.put(dst_stage , fg);
                    }
                }

                // Then update dependencies and the "root"
                // Note that after this operation "root" is not Coflow_root, we need to trim() the DAG.
                dag.updateDependency( src_stage , dst_stage);

            } // end of current DAG
            // trim the DAG (remove the "root" -> null)
            dag.trimRoot();

            // flush the Coflows from the buffer to dag, then add to dagList
            dag.addCoflows(tmpCoflowList);
            dagList.add(dag);

        } // end of trace.txt

        br.close();
        return dagList;

    }

}
