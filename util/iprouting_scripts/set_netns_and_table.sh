#!/bin/bash

# first detect the hostname
host=$(hostname -s)

case "$host" in
    "hk") host_id=1 ;;
    "la") host_id=2 ;;
    "ny") host_id=3 ;;
    "fl") host_id=4 ;;
    "ba") host_id=5 ;;
    *) echo "Wrong hostname!"
    exit;;
esac

#debug
set -o xtrace

sudo sysctl -w net.ipv4.ip_forward=1

# then set the netns and the vlans
net_name=$host
echo "using netns: $net_name"

sudo ip netns add $net_name

sudo ip link add $host-eth0 type veth peer name vgw

sudo ip link set vgw netns $net_name

sudo ifconfig $host-eth0 10.0.$host_id.$host_id/24
sudo ip netns exec $net_name ifconfig vgw 10.0.$host_id.254/24

sudo ip route add 10.0.0.0/16 via 10.0.$host_id.254
    
sudo bash -c "echo '1   T1' >> /etc/iproute2/rt_tables"
sudo ip netns exec $net_name ip route add default via 10.0.$host_id.254 dev vgw table T1
# T1 table only need this default rule

sudo ip netns exec $net_name ip rule add from all fwmark 1 table T1

counter=1

# traverse the vlan and set them up
# need to sort the list of vlans according to the link_id
ip addr | grep -F '10.10' | sort -n | while read line ; do
    nic_addr=$(echo $line | awk '{print $2}');
    nic_name=$(echo $line | awk '{print $7}');
    echo "setting " $nic_addr : $nic_name
    
    # move to netns and then reset the IP address
    sudo ip link set $nic_name netns $net_name
    sudo ip netns exec $net_name ifconfig $nic_name $nic_addr
    
    # set the table now
    let counter++
    sudo bash -c "echo '$counter   T$counter' >> /etc/iproute2/rt_tables"
    
#     calculate the IP addr across this link, and then set as the default gateway!
    link_id=$(echo $nic_addr | gawk -F . '{print $3}')
    
    # remove suffix
    gw_ip=10.10.$link_id.${link_id/$host_id/}
    
    sudo ip netns exec $net_name ip route add default via $gw_ip dev $nic_name table T$counter
    
    sudo ip netns exec $net_name ip rule add from all fwmark $counter table T$counter
    
done

echo "done"