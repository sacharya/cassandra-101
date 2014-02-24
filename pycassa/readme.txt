sudo apt-get install python-dev python-virtualenv
virtualenv ~/.env
source ~/.env/bin/activate
pip install pycassa

python cassandra-pycassa.py
