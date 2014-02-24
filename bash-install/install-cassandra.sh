#!/bin/nash
sudo apt-get update
sudo apt-get upgrade

# http://wiki.apache.org/cassandra/DebianPackaging
sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 20x main" >> /etc/apt/sources.list'
sudo sh -c 'echo "deb-src http://www.apache.org/dist/cassandra/debian 20x main" >> /etc/apt/sources.list'

gpg --keyserver wwwkeys.pgp.net --recv-keys 4BD736A82B5C1B00
sudo apt-key add ~/.gnupg/pubring.gpg
sudo apt-get update

#sudo apt-get --purge remove cassandra

sudo apt-get install cassandra

IP=`ifconfig eth1 | grep inet | head -n1 | cut -d":" -f2 | cut -d" " -f1`
sudo sed -i "s/listen_address: localhost/listen_address: ${IP}/" /etc/cassandra/cassandra.yaml
sudo sed -i "s/rpc_address: localhost/rpc_address: ${IP}/" /etc/cassandra/cassandra.yaml

read -p "Enter the IP or IPs (comma separated) of the seed node(s), or 127.0.0.1 if its the first node in the cluster: " SEED_IPS
sudo sed -i "s/seeds: \"127.0.0.1\"/seeds: \"${SEED_IPS}\"/" /etc/cassandra/cassandra.yaml

sudo /etc/init.d/cassandra restart

nodetool status
