#!/usr/bin/env bash

# define the behavior here
setovs(){

    agentID=$1

    echo 'this is agent ' $agentID

    set -o xtrace

    ovs-vsctl add-br s$agentID
    ovs-vsctl set bridge s$agentID other-config:datapath-id=000000000000000$agentID

    ip link add h$agentID-eth0 type veth peer name s$agentID-eth1
    ip link set s$agentID-eth1 up

    ifconfig h$1-eth0 10.$agentID netmask 255.255.255.0

    ovs-vsctl add-port s$agentID s$agentID-eth1

    shift

    ctrlIP=$1

    shift

    for vlan in "$@"
    do
        ifconfig $vlan 0.0
        ip addr flush dev $vlan
        ip -6 addr flush dev $vlan
        ovs-vsctl add-port s$agentID $vlan
    done

    ovs-vsctl set-controller s$agentID tcp:$ctrlIP

}

echo 'IMPORTANT: in ovs setup agent id starts with 1, although in experiments agent id starts with 0'
echo 'IMPORTANT: run this script with ROOT'

echo 'input format: [ID] [CTRL_IP] [vLANs]'

echo 'read input ' $@
read -p "Are you sure you want to continue? <y/N> " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]
then
    setovs $@
else
    exit 0
fi


