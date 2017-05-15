#!/bin/bash

# This script is used to download and build codes
# to perform  emulation.

sudo apt install -y nload bwm-ng htop pv
export LC_ALL=en_US.UTF-8
sudo pip install networkx

echo "mounting /tmp as tmpfs..."
sudo mount -t tmpfs tmpfs /tmp

rsync -avh /proj/gaia-PG0/gaia/floodlight/floodlight/ ~/new

echo "Floodlight ready for use, now compiling gaiasim..."

cd ~/gaiasim
mvn package

echo "Gaiasim now ready, preparing output folders..."
./util/make_output_dir.sh
