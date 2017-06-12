#!/bin/bash

# Script ver 1.1
# This script is used to download and build codes

#sudo apt install -y nload bwm-ng htop pv
#export LC_ALL=en_US.UTF-8
#sudo pip install networkx

echo "mounting /tmp as tmpfs..."
sudo mount -t tmpfs tmpfs /tmp

#echo "Copying floodlight into home folder"
#rsync -avh /proj/gaia-PG0/gaia/floodlight/floodlight/ ~/floodlight
#cp -rp /usr/local/share/floodlight/ ~/floodlight

echo "Compiling GAIA"
cd ~/gaiasim
mvn package

echo "Gaiasim now ready, preparing output folders..."
./util/make_output_dir.sh
