from link import Link, LinkDict
import networkx as nx

from mininet.net import Mininet
from mininet.node import OVSSwitch

class NetGraph(object):
    def __init__(self, linkData=None, nodesData=None, gmlfilename=None, defaultbandwidth=1000):
        # links: {node : {neighbor : {"bandwith" : bw, "status" : "free/busy", "available" : time}}}
        self.links = {}
        self.nodes = []
        self.node_label_by_id = {}
        self.node_id_by_label = {}
        self.link_dict = LinkDict()
        self.G = None

        self.mininet_hosts = {}
        self.mininet_host_switches = {}
        self.interfaces = {}
        self.mininet_host_ips = {}

        self.G = nx.read_gml(gmlfilename, label="id")
        for key, data in self.G.nodes(data=True):
            self.nodes.append(data["label"])
            self.node_label_by_id[key] = data["label"]
            self.node_id_by_label[data["label"]] = key
        for (start_node, end_node, link_labels_dict) in self.G.edges(data=True):
            start_label = self.node_label_by_id[start_node]
            end_label = self.node_label_by_id[end_node]
            if 'bandwidth' in link_labels_dict:
                self.link_dict.add(Link(start=start_label, end=end_label, bandwidth=int(link_labels_dict['bandwidth'])))
            else:  # use the default value specified
                self.link_dict.add(Link(start=start_label, end=end_label, bandwidth=int(defaultbandwidth)))
       
        # Set up map for link interfaces on each switch
        # and host IP mappings
        max_interface_numbers = {}
        for id, data in self.G.nodes(data=True):
            key = self.node_label_by_id[id]
            self.interfaces[key] = {}
            self.mininet_host_ips[key] = '10.0.0.' + str(id)

            # Each switch has port 1 connected to their dedicated switch
            self.interfaces[key][key] = 1
            max_interface_numbers[key] = 1
        
        max_interface_numbers['CTRL'] = 1

        # Populate interface map so that we can later query it when setting
        # up Openflow rules
        for n1, n2 in self.link_dict.get_dict():
            next_if1 = max_interface_numbers[n1] + 1
            max_interface_numbers[n1] += 1

            # This assumes that there's only one direct link between
            # each switch.
            assert(n2 not in self.interfaces[n1])
            self.interfaces[n1][n2] = next_if1

            next_if2 = max_interface_numbers[n2] + 1
            max_interface_numbers[n2] += 1

            # This assumes that there's only one direct link between
            # each switch.
            assert(n1 not in self.interfaces[n2])
            self.interfaces[n2][n1] = next_if2

        # Add the interface connecting switches to the controller switch
        self.interfaces['CTRL'] = {}
        for id, data in self.G.nodes(data=True):
            key = self.node_label_by_id[id]
            next_if = max_interface_numbers[key] + 1
            max_interface_numbers[key] += 1
            self.interfaces[key]['CTRL'] = next_if

            next_if = max_interface_numbers['CTRL'] + 1
            max_interface_numbers['CTRL'] += 1
            self.interfaces['CTRL'][key] = next_if

    def mininet_init_topo(self, net):
        switch_name_to_id = {}
        
        # Create controller node and switch
        ctrl_ip_suffix = len(self.nodes) + 1
        ip_block = '10.0.0.' + str(ctrl_ip_suffix)
        ctrl = net.addHost('CTRL', ip=ip_block)
        switch_id = "s" + str(ctrl_ip_suffix)
        ctrl_switch = net.addSwitch(switch_id, protocols=['OpenFlow13'], cls=OVSSwitch)
        net.addLink('CTRL', switch_id, bw=9999)

        for id, data in self.G.nodes(data=True):
            key = self.node_label_by_id[id]

            # Create hosts for each node
            ip_block = self.mininet_host_ips[key]
            h = net.addHost(key, ip=ip_block)
            self.mininet_hosts[key] = h
            
            switch_id = "s" + str(id)
            s = net.addSwitch(switch_id, protocols=['OpenFlow13'], cls=OVSSwitch)
            self.mininet_host_switches[key] = s
            switch_name_to_id[key] = switch_id
        
            # Connect each host switch to each host
            net.addLink(key, switch_id, bw=9999)

        # Connect switches based on topology links
        for n1, n2 in self.link_dict.get_dict():
            bandwidth = self.link_dict.get(start_node_id=n1, 
                                           end_node_id=n2).bandwidth

            print "Adding " + n1 + "-" + n2 + " link with bw = " + str(bandwidth)
            # TODO(jack): Determine metric for setting link delay
            net.addLink(switch_name_to_id[n1],
                        switch_name_to_id[n2], bw=int(bandwidth))

        # Connect the controller to all switches
        # TODO: Figure out a more realistic connectivity
        for host, switch in sorted(self.mininet_host_switches.items()):
            print "Adding CTRL-" + host + " link"
            net.addLink(ctrl_switch, switch, bw=9999)

        return ctrl
