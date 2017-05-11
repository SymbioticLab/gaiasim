# GAIA
This repository contains the source code for the simulation and emulation of of jobs in GAIA, an approach to reduce job completion times for geo-distributed dataBroker analytics by co-optimizing flow routing and scheduling.

## Table of Contents
1. [Quick Start](#quick-start)

2. [Controller](#controller)

	2.1 [Jobs](#jobs)
	
	2.2 [Scheduling](#scheduling)
	
3. [Simulation](#simulation)

4. [Emulation](#emulation)

	4.1 [Emulated Topology](#emulated-topology)

	4.2 [Architecture](#architecture)
	
	4.3 [Example](#example)
	
	4.4 [Common Problems in Emulation](#common-problems-in-emulation)

## Quick Start

To build from the command line, run:

```
mvn package
```

To run from the command line, run:

```
java -cp target/gaia_ctrl-jar-with-dependencies.jar gaiasim.GaiaSim -g <path_to_gml_file> -j <path_to_trace_file> -s <scheduler_type> -o <output_directory>
```

Command line arguments are as follows:

```
-g : Path to the gml file used for the topology
-j : Path to the trace file used for submitting jobs
-s : Type of scheduler to use (currently one of { baseline, recursive-remain-flow })
-o : (optional) Path to directory to save statistics output. If not specified, default is /tmp
-e : (optional) Run under emulation
```

At the end of simulation/emulation, job completion times and coflow completion times will be saved to <output_directory>/job.csv and <output_directory>/cct.csv. If the `-o` flag is not used in the command above, the default directory is /tmp.

## Controller

When running the command shown above, you're starting the GAIA Controller. The Controller receives traffic matrices from jobs and determines when flows for the jobs should be scheduled, on what path(s) flows should traverse, and how much bandwidth should be allocated to flows.

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

### Scheduling
Flows are available to be scheduled when all tasks in their source stage have completed. Since GAIA operates over a WAN, tasks are expected to take insignificant amounts of time compared to shuffles, and thus we model stages as completing instantaneously in simulation and emulation. Thus, flows with source stage N can begin once all flows with destination stage N have completed. The Controller schedules flows differently depending on the scheduler being used (`-s { baseline, recursive-remain-flow }`).

#### Baseline
The baseline scheduler (enabled using `-s baseline`) uses minimal information about the WAN topology and other concurrent flows when scheduling flows. When a flow is available to begin, the baseline scheduler will start it. In simulation, we allow the flow to take the path that has the highest maximum bandwidth.

The module responsible for scheduling in the `baseline` mode is src/main/java/gaiasim/scheduler/BaselineScheduler.java.

#### Coflow
The coflow scheduler (enabled using `-s recursive-remain-flow`) extensively considers the WAN topology and concurrently running flows when scheduling flows. It attempts to schedule flows so as to minimize coflow-completion-time (shuffle completion time) and considers multiple paths between the source and destination of a flow. 

The scheduler reschedules all running flows whenever a new coflow is available to be scheduled or when a coflow has completed (generally a coflow completing will lead to a new coflow being available). The scheduler attempts to schedlule all flows in a coflow at the same time. If there is not sufficient bandwidth remaining for an entire coflow to be scheduled, individual flows of coflows will be scheduled to occupy remaining bandwidth.

Coflows are allocated bandwidth on the WAN in order of increasing expected coflow completion time. In order to determine expected coflow completion time, the scheduler solves a linear program (LP) involving the maximum bandwidth available on each link in the topology. Each active coflow is run through the LP using maximum link bandwidths to determine the coflow's expected completion times if the coflow were the only coflow running on the WAN. Coflows are then sorted in increasing order of expected completion time and then allocated until there is insufficient bandwidth remaining. An in-depth description of the LP solved by the scheduler may be found in docs/calculating_cct.pdf.

Allocating a coflow on the WAN involves determining the paths and bandwidths that will be taken by each of the flows in the coflow. Flows can be split to take multiple paths. As flows are allocated paths and bandwidths on the WAN, we reduce the bandwidth available on topology links involved in the allocation. Subsequent coflows that are to be allocated during this round of scheduling will once again solve the LP described above, but with a topology that reflects the reduced link bandwidths.

The module responsible for scheduling in the `recursive-remain-flow` mode is src/main/java/gaiasim/scheduler/PoorManScheduler.java (named continued from the previous version of the simulator) and the module that sets up and solves the LP is src/main/java/gaiasim/mmcf/MMCFOptimizer.java. Solving the LP requires that the system has glpk installed.

## Simulation

When running GAIA under simulation (without the `-e` flag), the network is simulated. This means that there is no interaction with the Linux networking stack or network devices (e.g., routers and switches). There is no connection startup overhead, or network congestion.

The GAIA Controller uses time-based simulation. It operates iteratively in a loop until all jobs are completed, where each iteration is some simulated timestep (e.g., 1ms). During each iteration, the Controller will check for any started jobs or completed flows/coflows, make scheduling updates if need be, and progress any active flows. Each active flow is allocated some bandwidth, and thus is progressed by (bandwidth / timestep) bits each iteration.

## Emulation

When running GAIA under emulation (with the `-e` flag), Mininet is used to emulate the WAN topology provided with the `-g` flag. Mininet creates a virtual network with one host per node in the WAN topology and links between the nodes according to edges specified in the topology. Each Mininet host will run its own Linux networking stack, and the emulated network will contain relevant network devices (e.g., routers and switches). Thus, the emulation provides a much more realistic setting; network randomness is more realistic, there is connection startup overhead, etc.

### Emulated Topology

To set up a topology in Mininet, we use the following command:

```
sudo python mininet/setup_topo.py -g <path_to_gml_file> -s <scheduler_type>
``` 
where the `-g` and `-s` flags are consistent with those in earlier commands.

This script creates a Mininet host for each node in the topology and adds a Controller host (named "CTRL") to the topology. Each created host is also given its own switch. Switches assigned to certain hosts are then linked together according to the edges specified in by the gml file.

NOTE: The default Mininet installation does not support large link bandwidths. To support link bandwidths greater than 1 Gbps, one should use the custom Mininet repo found here: https://github.com/jackkosaian/mininet. If you use the setup script mininet/install.sh, this should already be handled for you.

#### Current Limitations
Currently, the Controller has a direct connection to all hosts, which is unrealistic.

WAN link latencies currently are not accounted for in setting up the topology. This shouldn't be a huge problem, though, as most flows in our scenario will be throughput-sensitive rather than latency-sensitive.

### Architecture

The GAIA Controller needs to be able to instruct topology nodes to start flows, update flows, and provide status about flows. To manage this, each node in the topology runs a SendingAgent and ReceivingAgent. The job of the SendingAgent is to send flows on behalf of the controller. The SendingAgent will send flows along the path(s) specified by the controller and at the bandwidths allocated to them. ReceivingAgents are responsible for receiving dataBroker from other hosts.

#### SendingAgents

On initialization, the GAIA Controller establishes TCP connections to each of the SendingAgents in the topology. The Controller will use these connections to send and receive messages about flows being sent by SendingAgents. SendingAgents operate differently dependingon which scheduling mode (`-s` flag) is used.

##### Baseline

When using the baseline scheduler (`-s baseline`), the Controller simply tells a SendingAgent which ReceivingAgent a flow is to be sent to, and how much dataBroker is to be sent. The SendingAgent will then open a TCP connection with the specified ReceivingAgent and send dataBroker until reaching the amount specified by the Controller. When the flow has finished sending all of its dataBroker, the TCP connection with the ReceivingAgent is terminated and the SendingAgent sends a message back to the Controller indicating that the flow has finished.

The module used to run the baseline SendingAgent is src/main/java/gaiasim/agent/BaselineSendingAgent.java

##### Coflow

Communication between the Controller and SendingAgents is much more nuanced when using coflow scheduling (`-s recursive-remain-flow`). When the Controller wishes for a SendingAgent to send a flow, it must tell the SendingAgent not only the ReceivingAgent to which the flow will be sent and the amount of dataBroker in the flow, but also the paths on which the flow will be sent and the bandwidth allocated for the flow on each path.

Rather than starting a new TCP connection for each flow being sent by a SendingAgent, a SendingAgent will maintain persistent connections between it and a ReceivingAgent. Currently, the SendingAgent will maintain one persistent connection per path available between it and a ReceivingAgent. Thus, if there are two paths between nodes A and B, A will maintain two persistent connections with B. In the future, one could have two pools of persistent connections in this scenario.

In order for the Controller's scheduling decisions to actually be enforced, we need to ensure that packetes for flows actually traverse their assigned paths. We leverage a custom module from the Floodlight SDN controller to acheive this (found at https://github.com/jackkosaian/floodlight). On initialization, each SendingAgent establishes its persistent connections to ReceivingAgents and reports back to the GAIA Controller the port numbers used by these connections. The GAIA Controller then relays this information to the Floodlight Controller so that it can install OpenFlow forwarding rules on switches in the topology. Installed rules will match on <sending_agent_ip, sending_agent_port, receiving_agent_ip, receiving_agent_port> and will output packets through the interface that leads to the next hop in the designated path. After rules are installed for a certain persistent connection of a SendingAgent, all packets sent on that connection will traverse the path set up by the Floodlight controller.

At any given time, multiple flows may be allocated by the Controller to be sent along a certain path. For example, the Controller might schedule flows A and B on path X (4 Gbps max) with allocated bandwidths of 1 Gbps and 3 Gbps, respectively. Since these flows are to take the same path, they should both be sent using the persistent connection that represents path X. Rather than having both flows attempt to concurrently send through the socket for path X and rate limiting themselves, we have the SendingAgent send a buffer of dataBroker through the socket, and then attribute the amount of dataBroker sent based on the bandwidth allocations given to each flow. In our example, if the SendingAgent sends 8 MB of dataBroker, then flow A will be considered to have advanced by 8 * (1 / 4) = 2 MB, and flow B will be considered to have advanced by 8 * (3 / 4) = 6 MB. Once a flow has completed, the SendingAgent will send a message to the Controller.

Recall that the coflow scheduler reschedules all running flows whenever a coflow is available to begin or completes. When it performs this rescheduling, the scheduler needs to know how much progress has been made on active flows. To do so, the Controller sends a status request message to each SendingAgent. Each SendingAgent will send back messages for all flows that it is currently managing containing information about the amount that has been transmitted. After receiving all status response messages from SendingAgents, the Controller will reschedule active flows and send updates to SendingAgents with the new paths and allocations for flows.

It can take a large amount of time for the Controller to reschedule flows, as it makes many calls to glpk to solve the LP. To reduce the impact of this time spent rescheduling, SendingAgents continue to send dataBroker for active flows while the Controller is calculating new flow allocations. This means that the update messages sent by the Controller to a SendingAgent after rescheduling a flow might be a bit inconsistent with the status of the flow as seen by the SendingAgent. In some cases, the SendingAgent may even have finished sending the flow by the time it receives an update message from the Controller. In such cases, the SendingAgent simply sends back a message to the Controller saying that the flow has finished -- from the Controller's perspective, this appears as though the flow finished after the update.

The module used to run the coflow SendingAgent is src/main/java/gaiasim/agent/PersistentSendingAgent.java. Note that the SendingAgent has no knowledge of coflows -- that is left to the GAIA Controller.

### Example

Start up a our custom floodlight controller in one terminal as follows:

```
cd ~/floodlight
java -jar target/floodlight.jar
```

In another terminal (or screen session) run the following:

```
cd ~/gaiasim
sudo python mininet/setup_topo.py -g dataBroker/swan.gml -s recursive-remain-flow
```

This should start up a Mininet prompt. Test connectivity by running the following:

```
mininet> pingall
```

This should result in all hosts being able to contact one another. If not, try restarting the process.

Once Mininet is up and running, start the GAIA Controller by doing the following:

```
mininet> CTRL java -cp target/gaia_ctrl-jar-with-dependencies.jar gaiasim.GaiaSim -g dataBroker/swan.gml -j dataBroker/simple_jobs/simple_trace-swan.txt -s recursive-remain-flow
```

This should run to completion and output values regarding JCT and CCT to /tmp/job.csv and /tmp/cct.csv, respectively.

### Common Problems in Emulation

Mininet requires a large amount of memory to support a large number of hosts and a large amount of total topology bandwidth. If you find yourself continuously having problems getting `pingall` to work correctly or are unable to drive the expected amount of bandwidth between two nodes (by using `iperf`), consider running on a machine with more memory.

If emulation crashes and takes you out of the Mininet shell (you can tell whether you're in the Mininet shell by the prompt being `mininet>`), Mininet likely did not exit properly, and may need to be cleaned up before starting again. To do this, run `sudo mn -c` before trying to start the topology again.