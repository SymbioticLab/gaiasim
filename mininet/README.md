Python scripts used to set up a topology in Mininet

A topology may be set up as follows:
```
sudo python setup_topo.py -g <path_to_gml_file> -s <scheduler_type>
```

Command line arguments are as follows:
```
-g : Path to the gml file used for the topology
-s : Type of scheduler to use (currently one of { baseline, recursive-remain-flow })
```
