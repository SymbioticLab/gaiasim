#/bin/bash

echo "removing $HOME/floodlight and fetching a new one..."

cd ~
rm -rf floodlight

git clone https://github.com/jackkosaian/floodlight.git
cd floodlight
git submodule init
git submodule update
ant 
sudo mkdir /var/lib/floodlight
sudo chmod 777 /var/lib/floodlight 
