#!/bin/bash
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.2.2 --sport 40001 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.1.1 --sport 33330 --dport 40001 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.3.3 --sport 40007 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.1.1 --sport 33330 --dport 40007 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.4.4 --sport 40009 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.1.1 --sport 33330 --dport 40009 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.4.4 --sport 40012 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.1.1 --sport 33330 --dport 40012 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40015 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40015 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40016 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40016 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40017 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40017 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40018 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40018 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40019 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40019 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40020 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40020 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 40021 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 33330 --dport 40021 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.1.1 --sport 40103 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.2.2 --sport 33330 --dport 40103 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.3.3 --sport 40107 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.2.2 --sport 33330 --dport 40107 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.4.4 --sport 40109 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.2.2 --sport 33330 --dport 40109 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.4.4 --sport 40111 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.2.2 --sport 33330 --dport 40111 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40114 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40114 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40115 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40115 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40116 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40116 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40117 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40117 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40118 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40118 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 40119 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 33330 --dport 40119 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.1.1 --sport 40201 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.3.3 --sport 33330 --dport 40201 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.2.2 --sport 40205 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.3.3 --sport 33330 --dport 40205 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.4.4 --sport 40209 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.3.3 --sport 33330 --dport 40209 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 40213 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 33330 --dport 40213 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 40214 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 33330 --dport 40214 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 40215 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 33330 --dport 40215 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 40216 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 33330 --dport 40216 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.1.1 --sport 40301 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.4.4 --sport 33330 --dport 40301 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.1.1 --sport 40302 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.4.4 --sport 33330 --dport 40302 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.2.2 --sport 40307 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.4.4 --sport 33330 --dport 40307 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.2.2 --sport 40308 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.4.4 --sport 33330 --dport 40308 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.3.3 --sport 40312 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.4.4 --sport 33330 --dport 40312 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 40316 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 33330 --dport 40316 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 40317 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 33330 --dport 40317 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 40318 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 33330 --dport 40318 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 40319 --dport 33330 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 33330 --dport 40319 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40401 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40401 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40402 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40402 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40403 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40403 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40404 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40404 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40405 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40405 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40406 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40406 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.1.1 --sport 40407 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.1.1 -d 10.0.5.5 --sport 33330 --dport 40407 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40408 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40408 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40409 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40409 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40410 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40410 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40411 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40411 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40412 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40412 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.2.2 --sport 40413 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.2.2 -d 10.0.5.5 --sport 33330 --dport 40413 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 40414 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 33330 --dport 40414 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 40415 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 33330 --dport 40415 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 40416 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 33330 --dport 40416 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.3.3 --sport 40417 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.3.3 -d 10.0.5.5 --sport 33330 --dport 40417 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 40418 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 33330 --dport 40418 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 40419 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 33330 --dport 40419 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 40420 --dport 33330 -j MARK --set-mark 2 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 33330 --dport 40420 -j MARK --set-mark 1 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.5.5 -d 10.0.4.4 --sport 40421 --dport 33330 -j MARK --set-mark 3 
sudo ip netns exec ba iptables -A PREROUTING -t mangle -p tcp -s 10.0.4.4 -d 10.0.5.5 --sport 33330 --dport 40421 -j MARK --set-mark 1 

#sudo ip netns exec ba iptables -A PREROUTING -t mangle -d 10.0.1.1 -j MARK --set-mark 2
#sudo ip netns exec ba iptables -A PREROUTING -t mangle -d 10.0.2.2 -j MARK --set-mark 3
#sudo ip netns exec ba iptables -A PREROUTING -t mangle -d 10.0.3.3 -j MARK --set-mark 2
#sudo ip netns exec ba iptables -A PREROUTING -t mangle -d 10.0.4.4 -j MARK --set-mark 3
#sudo ip netns exec ba iptables -A PREROUTING -t mangle -d 10.0.5.5 -j MARK --set-mark 1
