#!/bin/bash

#Overlay 1
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.1.1 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.2.2 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.3.3 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.4.4 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.5.5 -j MARK --set-mark 1

#Overlay 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.1.1 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.2.2 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.3.3 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.4.4 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.5.5 -j MARK --set-mark 1

#Overlay 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.1.1 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.2.2 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.3.3 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.4.4 -j MARK --set-mark 3
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.5.5 -j MARK --set-mark 1
