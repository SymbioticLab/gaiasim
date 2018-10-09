## GAIA Simulator
*IMPORTANT*: you must have glpk v4.57 installed and `glpsol` must be in your `$PATH` before running.

For a clean build from the command line, run:
```
mvn clean package
```

To run from the command line, run:
```
java -cp target/gaiasim-*-SNAPSHOT-jar-with-dependencies.jar gaiasim.GaiaSim -g <path_to_gml_file> -j <path_to_trace_file> -s <scheduler_type> -o <output_directory> -b <bandwidth_scaling> -w <workload_scaling>
```
For example:
```
java -cp target/gaiasim-*-SNAPSHOT-jar-with-dependencies.jar gaiasim.GaiaSim -g data/gml/swan.gml -j data/combined_traces_fb/BigBench-swan.txt -s recursive-remain-flow -o /tmp -b 1 -w 1
```

Command line arguments are as follows:
```
-g : Path to the gml file used for the topology
-j : Path to the trace file used for simulation
-s : Type of scheduler to use (currently one of { baseline, recursive-remain-flow, multipath, varys, swan, dark })
-o : (optional) Path to directory to save statistics output. If not specified, default is /tmp
-b : multiplicative scaling factor for edge bandwidths
-w : multiplicative scaling factor for transfer sizes in workloads
```


### Jobs
Jobs (DAGs of MapReduce-like tasks) are submitted to the Controller over time as specified by the trace file used. Each job consists of a number of stages (Maps and Reduces), as well as information about the amount of dataBroker transferred (shuffled) between stages.

Individual jobs in a trace file should be of the following form:

```
<num_tasks> <job_id> <job_arrival_time>
<stage0_name> <num_tasks> (optional)<location_ids>
<stage1_name> <num_tasks> (optional)<location_ids>
.
.
<num_shuffles>
<src_stage0_name> <dst_stage0_name> <num_mb_shuffled>
<src_stage1_name> <dst_stage1_name> <num_mb_shuffled>
.
.
```

Let's take a look at an example job, job 1 from the BigBench-swan.txt trace:

```
4 1 11
Map1 1 HK
Map4 1 BA
Reducer3 2 HK NY
Reducer2 1 NY
3
Map1 Reducer2 1154
Map4 Reducer2 497
Reducer2 Reducer3 1227
```

The first line tells us that the job has id 1, 4 stages, and will arrive at time 11 (11 seconds after the start of the trace). 

Because there are 4 stages, the next 4 lines contain descriptions about the stages and their locations. For example, the line "Reducer3 2 HK NY" tells us that stage Reducer3 has 2 tasks which take place in locations HK and NY. Note that the trace could have omitted location information and simply read "Reducer3 2". If location information is omitted, the Controller will assign the tasks to random locations.

After the 4 lines for the 4 stages, the line containing "3" tells us that there are 3 shuffles between these stages. The next 3 lines will describe these shuffles. For example, the line "Reducer2 Reducer3 1227" tells us that 1227 MB are to be shuffled between stage Reducer2 and Reducer3. A flow will be created between each pair of tasks between the two stages. For example, if a source stage has M tasks and a destination stage has N tasks, then the shuffle will contain M*N flows with shuffle volume being distributed evenly among these flows. In this example line, there will be two flows: NY-HK and NY-NY and each flow will be responsible for sending 1227 / 2 = 613.5 MB. Note that for the second flow, the two tasks are in the same location. If this takes place, the flow is ignored in scheduling as no volume actually needs to be transferred over the WAN.

The module responsible for parsing trace files may be found in src/main/java/gaiasim/spark/DAGReader.java. It reads a trace file and constructs the DAGs for each job, including stage dependencies and flows in each shuffle. Trace files are processed in entirety before the Controller begins scheduling flows.
