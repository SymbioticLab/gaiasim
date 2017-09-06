#!/bin/bash
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -d 10.0.1.1 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -d 10.0.2.2 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -d 10.0.3.3 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -d 10.0.4.4 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -d 10.0.5.5 -j MARK --set-mark 1
