*IMPORTANT*: you must have glpk v4.57 installed and `glpsol` must be in your `$PATH` before running.

For a clean build from the command line, run:
```
mvn clean package
```

To run from the command line, run:
```
java -cp target/gaiasim-*-SNAPSHOT-jar-with-dependencies.jar gaiasim.GaiaSim -g <path_to_gml_file> -j <path_to_trace_file> -s <scheduler_type> -o <output_directory> -b <bandwidth_scaling> -w <workload_scaling>
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
