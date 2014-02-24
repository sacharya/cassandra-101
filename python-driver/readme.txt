sudo apt-get install python-dev python-virtualenv
virtualenv ~/.env
source ~/.env/bin/activate
pip install cassandra-driver

python cql.py
