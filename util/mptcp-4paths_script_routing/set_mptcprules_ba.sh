#!/bin/bash

#Overlay 1
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.1.1 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.2.2 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.3.3 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.4.4 -j MARK --set-mark 2
sudo ip netns exec ba iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.5.5 -j MARK --set-mark 1

#Overlay 2

#1-2 NO BA

#1-3 NO BA

#1-4 NO BA

#1-5 NO

#2-3 NO BA

#2-4 NO BA

#2-5
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.2.2 -d 10.1.5.5 -j MARK --set-mark 1
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.5.5 -d 10.1.2.2 -j MARK --set-mark 3

#3-4 NO BA

#3-5
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.3.3 -d 10.1.5.5 -j MARK --set-mark 1
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.5.5 -d 10.1.3.3 -j MARK --set-mark 3

#4-5
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.4.4 -d 10.1.5.5 -j MARK --set-mark 1
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.1.5.5 -d 10.1.4.4 -j MARK --set-mark 3



#Overlay 3

#3-4
sudo ip netns exec la iptables -A PREROUTING -t mangle -s 10.0.3.3 -d 10.1.4.4 -j MARK --set-mark 3
sudo ip netns exec la iptables -A PREROUTING -t mangle -d 10.0.3.3 -s 10.1.4.4 -j MARK --set-mark 2

