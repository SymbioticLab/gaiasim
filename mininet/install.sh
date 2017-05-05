#!/bin/bash

# This script is used to set up prerequites needed
# to perform single-machine emulation.

sudo apt-get update
sudo apt-get install -y git vim-nox python-setuptools python-all-dev flex bison traceroute

# Install Mininet
cd ~
git clone https://github.com/jimmyyou/mininet.git
cd mininet
./util/install.sh -fnv

sudo apt-get install -y python-pip

# Install modules needed by simulator
sudo pip install networkx
sudo apt-get install -y python-glpk
sudo apt-get install -y glpk-utils

# Install java
sudo apt-get install -y software-properties-common python-software-properties
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y oracle-java8-installer

# Install floodlight and prereqs
sudo apt-get install -y build-essential ant maven python-dev
cd ~
git clone https://github.com/jackkosaian/floodlight.git
cd floodlight
git submodule init
git submodule update
ant 
sudo mkdir /var/lib/floodlight
sudo chmod 777 /var/lib/floodlight
cd ~
