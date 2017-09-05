#!/bin/bash

echo "setting rules for HK"

interface=$(ip addr | grep vlan | grep '@' | cut -d ':' -f 2 | cut -d '@' -f 2 | head -n 1)
echo "setting virtual interface on $interface"

set -o xtrace

sudo ip link add link $interface name macv1 type macvlan
sudo ifconfig macv1 10.0.1.1/24

sudo sysctl -w net.ipv4.ip_forward=1

# then set the route

# routing tables for node 1
sudo ip route add 10.0.2.2 via 10.10.12.2
sudo ip route add 10.0.3.3 via 10.10.13.3
sudo ip route add 10.0.4.4 via 10.10.12.2
sudo ip route add 10.0.5.5 via 10.10.13.3
