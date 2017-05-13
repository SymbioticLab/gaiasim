#!/bin/bash

# This script is used to download and build codes
# to perform  emulation.

sudo apt install -y nload bwm-ng htop pv
sudo pip install networkx

sudo mount -t tmpfs tmpfs /tmp

cd ~
git clone https://github.com/jackkosaian/floodlight.git
cd floodlight
git submodule init
git submodule update
ant 
sudo mkdir /var/lib/floodlight
sudo chmod 777 /var/lib/floodlight

echo "Floodlight ready for use, now compiling gaiasim..."

cd ~/gaiasim
mvn package

echo "Gaiasim now ready, preparing output folders..."
./util/make_output_dir.sh