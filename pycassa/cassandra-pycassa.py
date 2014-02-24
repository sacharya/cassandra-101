import json

from pycassa import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import SystemManager
from pycassa.system_manager import SIMPLE_STRATEGY
from pycassa.system_manager import UTF8_TYPE
from pycassa.system_manager import ASCII_TYPE

import time

############################## Create Keyspace ################################
server_list = ['cassandra1:9160', 'cassandra2:9160', 'cassandra3:9160']
sys = SystemManager(server_list[0])
sys.list_keyspaces()
if 'entries' in sys.list_keyspaces():
    sys.drop_keyspace('entries')
sys.create_keyspace('entries', SIMPLE_STRATEGY, {'replication_factor': '1'})

############################## Connection Pooling ############################
#pool = ConnectionPool('entries', server_list=server_list, pool_size=20)
pool = ConnectionPool('entries', server_list=server_list)

############################## Create Column Family ###########################
sys.create_column_family('entries', 'Author', comparator_type=UTF8_TYPE)
author_cf = ColumnFamily(pool, 'Author')

################################ INSERT #######################################
# Insert a row with a Column
author_cf.insert('sacharya', {'first_name': 'Sudarshan'})
# Insert a row with multiple columns
author_cf.insert('sacharya1', {'first_name': 'Sudarshan', 'last_name':
'Acharya'})
# Insert multiple rows
author_cf.batch_insert({'rowkey1': {'first_name': 'Sudarshan', 'last_name':
'Acharya'},
                'rowkey2': {'first_name': 'Sudarshan', 'last_name': 'Acharya'}})
# Insert lots of individual rows
for i in range(100):
    author_cf.insert('sacharya'+str(i), {'first_name': 'sudarshan'+ str(i)})

author_cf.insert('1000', {'1': '1'})
time.sleep(5)

################################### GET #######################################
# Get the row for the rowkey
authors = author_cf.get('sacharya')
print authors

# Get value for column
print "Get value for column"
authors = author_cf.get('sacharya1', columns=['first_name'])
print authors

# Get the colums for the row key and column key
authors = author_cf.get('sacharya1', columns=['first_name', 'last_name'])
print authors

authors = author_cf.multiget('sacharya', 'sacharya1')
print authors

print "Printing the keys"
keys = authors.keys()
for k in keys:
    print authors.get(k)
print "Keys printed"

#authors = list(author.get_range().get_keys())
for value in author_cf.get_range():
    print value[0]

# Only if using OrderPreservingPartitioner. Default is RandomPartitioner, which
# does md5 on the key
#for value in author_cf.get_range(start='sacharya5', finish='sacharya10'):
#    print value[0]

################################# UPDATE ######################################
# UPDATE a column for an existing row
author_cf.insert('sacharya1', {'first_name': 'sudarshan_updated'})
print "Updating first_name for row key sacharya1"
authors = author_cf.get('sacharya1')
print authors

for author in authors:
    print dir(author)
    print type(author)

# Convert OrderedDict to Json
jsonresult = json.dumps(authors)
print "Converting OrderedDict to JSON"
print type(json.loads(jsonresult))
for key in json.loads(jsonresult).keys():
    print key
    print json.loads(jsonresult)[key]

author_cf.insert('sacharya1', {'last_name': 'acharya_added'})
print "Addinging last_name column for row key sacharya1"
authors = author_cf.get('sacharya1')
print authors

# the second column will override the first
author_cf.insert('sacharya1', {'first_name': 'sudarshan', 'first_name':
'acharya'})
print "Addinging two columns with same name but different values"
authors = author_cf.get('sacharya1')
print authors['first_name']

print "Converting to Dict"
mydict = dict([(attr_name, set([attr_values])) for attr_name, attr_values in
authors.iteritems()])
for key in mydict.keys():
    print mydict[key]

#################################LIST As Value ###############################
sys.create_column_family('entries', 'myname', comparator_type=ASCII_TYPE)
name_cf = ColumnFamily(pool, 'myname')

x = ['acharya1', 'acharya2']
name_cf.insert('sacharya3', {'last_name': x})
names3 = name_cf.get('sacharya3')
print "List as a value"
print names3
attrs = dict([(attr_name, set([attr_values])) for attr_name, attr_values in
names3.iteritems()])
name_cf.insert("sacharya3", {'last_name':
attrs['last_name'].append("acharya3")})
print name_cf.get('sacharya3')

################################# COUNT #######################################
# Count the number of columns for the row key
count=author_cf.get_count("sacharya1")
print count 

count=author_cf.multiget_count(["sacharya1","sacharya2"])
print count
################################## REMOVE #####################################
# Remove the column for the row key and column key
print "Removing the column last_name for row key sacharya1"
author_cf.remove('sacharya1', columns=['last_name'])

time.sleep(5)

authors = author_cf.get('sacharya')
print authors

# REMOVE the entire row
author_cf.remove('sacharya')
try:
    time.sleep(5)
    print "Getting object already deleted"
    author_cf.get('sacharya')
except Exception as e:
    print e

# Delete all data from column family
author_cf.truncate()

############################### DROP KEYSPACE #################################
sys.drop_keyspace('entries')
pool.dispose()

