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
    p.add_argument('-d', '--debug', help='Do not start Agents', action="store_true")
    
def main():
    p = argparse.ArgumentParser(
        description='Simulate coflow scheduling',
        formatter_class=argparse.RawTextHelpFormatter
    )
    setup_argparse(p)
    args = p.parse_args()
    
    #baseline = False
    if args.scheduler == "baseline":
        baseline = True
        print "Using baseline topology."
    else:
        baseline = False
        print "Using coflow topology."

    ng = NetGraph(gmlfilename=args.gml)

    net = Mininet(topo=None, build=False, link=TCLink)
    c0 = net.addController('c0', controller=RemoteController, ip="127.0.0.1", port=6653)

    ctrl = ng.mininet_init_topo(net)
    
    net.start()

    if not args.debug:
        print "Not debug mode, starting the agents..."
        # Start the receiving agents
        for key, host in ng.mininet_hosts.iteritems():
            print "Starting receiving agent " + host.name
            host.cmd('cd ~/gaiasim; java -cp target/gaia_ra-jar-with-dependencies.jar gaiasim.agent.ReceivingAgent &');


        # Start the sending agents
        for key, host in ng.mininet_hosts.iteritems():
            host_id = int(ng.mininet_host_ips[key].split('.')[-1]) - 1

            cmd_str = 'cd ~/gaiasim; java -cp target/gaia_sa-jar-with-dependencies.jar gaiasim.agent.SendingAgent '
            if baseline: # not supported for now
                # cmd_str += str(host_id) + ' 0 > /tmp/salog_' + str(host_id) + '.txt &'
                cmd_str += ' -i ' + str(host_id) + ' -g ' + args.gml + ' > /tmp/salog_' + str(host_id) + '.txt &'
            else:
                # cmd_str += str(host_id) + ' 1 ' + args.gml + ' > /tmp/salog_' + str(host_id) + '.txt &'
                cmd_str += ' -i ' + str(host_id) + ' -g ' + args.gml + ' > /tmp/salog_' + str(host_id) + '.txt &'

            print "Starting sending agent " + host.name + " id " + str(host_id) +" . using: " + cmd_str
            host.cmd(cmd_str)
    else:
        print "In debug mode, do not start the agents"
        
    # Start the simulator
    out_file = '~/Sim/out_' + args.scheduler
    """ctrl.cmd('export PYTHONPATH=$PYTHONPATH:~/Sim; cd ~/Sim; ./run_ryu_controller.sh ' + args.gml + ' ' + args.job + ' ' + args.scheduler + ' > ' + out_file + ' 2>&1 &')"""

    CLI(net)
    net.stop()

if __name__ == '__main__':
    main()
