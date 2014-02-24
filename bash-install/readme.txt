1. Get a bunch of servers ready.
 Ubuntu 12.04 with at least 8 GBs of RAM.

2. Create a stack user on each node.
adduser stack
echo "stack ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

3. Login as the stack user.
bash <(curl -s https://raw.github.com/sacharya/cassandra-101/master/bash-install/install-cassandra.sh)
