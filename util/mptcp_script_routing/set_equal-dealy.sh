#!/bin/bash

sudo ip netns exec $(hostname -s) tc qdisc show | grep vgw | while read line ; do
    handle=$(echo $line | awk '{print $3}');
    nic_name=$(echo $line | awk '{print $5}');
    echo "setting " $handle : $nic_name
    
    sudo ip netns exec $(hostname -s) tc qdisc change dev $nic_name handle $handle netem delay 10ms
done
