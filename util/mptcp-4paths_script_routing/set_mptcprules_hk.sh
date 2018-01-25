#!/bin/bash

# Overlay 1: match 10.0.0.0/16 to 10.0.0.0/16
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.1.1 -j MARK --set-mark 1
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.2.2 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.3.3 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.4.4 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.0.0.0/16 -d 10.0.5.5 -j MARK --set-mark 3

# Overlay 2: 10.1.0.0/16 to 10.1.0.0/16

#1-2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.1.1 -d 10.1.2.2 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.2.2 -d 10.1.1.1 -j MARK --set-mark 1

#1-3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.1.1 -d 10.1.3.3 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.3.3 -d 10.1.1.1 -j MARK --set-mark 1

#1-4
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.1.1 -d 10.1.4.4 -j MARK --set-mark 2
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.4.4 -d 10.1.1.1 -j MARK --set-mark 1

#1-5


#2-3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -s 10.1.2.2 -d 10.1.3.3 -j MARK --set-mark 3
sudo ip netns exec hk iptables -A PREROUTING -t mangle -d 10.1.2.2 -s 10.1.3.3 -j MARK --set-mark 2

#2-4 NO HK
#2-5 NO HK


#Overlay 3: match 10.2.0.0/16 NO HK

