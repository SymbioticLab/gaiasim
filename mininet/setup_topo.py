import argparse
from net_graph_mininet import NetGraph

from mininet.link import TCLink
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI

import os

def setup_argparse(p):
    p.add_argument('-g', '--gml', help='Network data in gml format', required=True)
    p.add_argument('-s', '--scheduler', help='One of {recursive-remain-flow, baseline}', required=True)
    
def main():
    p = argparse.ArgumentParser(
        description='Simulate coflow scheduling',
        formatter_class=argparse.RawTextHelpFormatter
    )
    setup_argparse(p)
    args = p.parse_args()
    
    baseline_str = '0'
    if args.scheduler == "baseline":
        baseline_str = '1'

    ng = NetGraph(gmlfilename=args.gml)

    net = Mininet(topo=None, build=False, link=TCLink)
    c0 = net.addController('c0', controller=RemoteController, ip="127.0.0.1", port=6633)

    ctrl = ng.mininet_init_topo(net)
    
    net.start()

    # Start the default controller
    """ctrl.cmd('export PYTHONPATH=$PYTHONPATH:~/Sim; ~/pox/pox.py l3_learning_ignore openflow.discovery --eat-early-packets openflow.spanning_tree --no-flood --hold-down openflow.of_01 --port=6633 > /dev/null 2>&1 &')"""

    # Wait for all switches to be connected to the default controller
    """net.waitConnected()

    for i in range(2):
        print "Iteration " + str(i+1)
        for key, host in ng.mininet_hosts.iteritems():
            ping_str = "Pinging from " + host.name

            result_str = host.cmd('ping 10.0.0.1 -c1')
            if "Host Unreachable" in result_str:
                ping_str += ": FAIL"
            else:
                ping_str += ": PASS"
            print ping_str"""

    # Start the receiving agents
    for key, host in ng.mininet_hosts.iteritems():
        print "Starting receiving agent " + host.name
        host.cmd('python ~/Sim/src/emulation/receiver.py > /dev/null 2>&1 &')

    
    # Start the sending agents
    for key, host in ng.mininet_hosts.iteritems():
        print "Starting sending agent " + host.name
        host.cmd('python ~/Sim/start_sending_agent.py ' + host.name + ' ' + args.gml + ' ' + baseline_str + ' > /dev/null 2>&1 &')
        
    # Start the simulator
    out_file = '~/Sim/out_' + args.scheduler
    """ctrl.cmd('export PYTHONPATH=$PYTHONPATH:~/Sim; cd ~/Sim; ./run_ryu_controller.sh ' + args.gml + ' ' + args.job + ' ' + args.scheduler + ' > ' + out_file + ' 2>&1 &')"""

    CLI(net)
    net.stop()

if __name__ == '__main__':
    main()
