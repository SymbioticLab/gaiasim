#!/bin/bash
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.0.1.1 -j MARK --set-mark 1
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.0.2.2 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.0.3.3 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.0.4.4 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.0.5.5 -j MARK --set-mark 3