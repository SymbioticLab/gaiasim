#!/bin/bash

# Overlay 1: match 10.0.0.0/16
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.1.1,10.1.1.1,10.2.1.1 -j MARK --set-mark 1
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.2.2,10.1.2.2,10.2.2.2 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.3.3,10.1.3.3,10.2.3.3 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.4.4,10.1.4.4,10.2.4.4 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.5.5,10.1.5.5,10.2.5.5 -j MARK --set-mark 3

#Overlay 2: match 10.1.0.0/16
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.1.1,10.1.1.1,10.2.1.1 -j MARK --set-mark 1
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.2.2,10.1.2.2,10.2.2.2 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.3.3,10.1.3.3,10.2.3.3 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.4.4,10.1.4.4,10.2.4.4 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.0.0/16 -d 10.0.5.5,10.1.5.5,10.2.5.5 -j MARK --set-mark 2

#Overlay 3: match 10.2.0.0/16
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.1.1,10.1.1.1,10.2.1.1 -j MARK --set-mark 1
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.2.2,10.1.2.2,10.2.2.2 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.3.3,10.1.3.3,10.2.3.3 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.4.4,10.1.4.4,10.2.4.4 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.2.0.0/16 -d 10.0.5.5,10.1.5.5,10.2.5.5 -j MARK --set-mark 2
