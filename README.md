To build from the command line, run:
```
mvn package
```

To run from the command line, run:
```
java -cp target/gaiasim-0.1.0-SNAPSHOT-jar-with-dependencies.jar gaiasim.GaiaSim -g <path_to_gml_file> -j <path_to_trace_file> -s <scheduler_type> -o <output_directory>
```

Command line arguments are as follows:
```
-g : Path to the gml file used for the topology
-j : Path to the trace file used for simulation
-s : Type of scheduler to use (currently one of { baseline, recursive-remain-flow, multipath, varys })
-o : (optional) Path to directory to save statistics output. If not specified, default is /tmp
```
