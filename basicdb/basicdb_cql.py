from cassandra.cluster import Cluster

import time
import uuid


class CassandraBackend(object):
    def __init__(self, base_keyspace='basicdb'):
        self.base_keyspace = base_keyspace
        server_list = ['cassandra1', 'cassandra2', 'cassandra3']
        self.cluster = Cluster(server_list)
        self.session = self.cluster.connect()
        self._create_keyspace()

    def _create_keyspace(self):
        result = self._execute(
            """
            SELECT * from system.schema_keyspaces WHERE keyspace_name='%s';
            """ % self.base_keyspace)
        
        self._execute("""DROP KEYSPACE %s;""" % self.base_keyspace)
        self._execute(
            """
            CREATE KEYSPACE %s 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor':
'1'};
            """ % self.base_keyspace)

    def _execute(self, query):
        print "====================================================="
        print query
        result = self.session.execute(query)
        print result
        return result

    def create_domain(self, owner, domain_name):
        self._execute(
            """
            CREATE TABLE %s.%s(
            domain text,
            PRIMARY KEY(domain)
            );
            """ % (self.base_keyspace, owner))
        self._execute(
            """
            INSERT INTO %s.%s (domain) VALUES ('%s');
            """ % (self.base_keyspace, owner, domain_name))
        self._execute(
            """
            CREATE TABLE %s.%s(
            item_name text,
            PRIMARY KEY(item_name));
            """ % (self.base_keyspace, domain_name))

    def delete_domain(self, owner, domain_name):
        return self._execute(
            """
            DELETE FROM %s.%s WHERE domain='%s';
            """ % (self.base_keyspace, owner, domain_name))

    def list_domains(self, owner):
        return self._execute("""SELECT * FROM %s.%s;""" % (self.base_keyspace,
owner))

    def domain_metadata(self, owner, domain_name):
        count = self._execute(
            """
            SELECT COUNT(*) FROM %s.%s;
            """ % (self.base_keyspace, domain_name))
        return {"ItemCount": count,
            "ItemNamesSizeBytes": '120',
            "AttributeNameCount": '12',
            "AttributeNamesSizeBytes": '120',
            "AttributeValueCount": '120',
            "AttributeValuesSizeBytes": 'rackspace20',
            "Timestamp": str(int(time.time()))}

    def add_attribute_value(self, owner, domain_name, item_name, attr_name,
attr_value):
        #execute(
        #    """SELECT * from system.schema_columnfamilies where
        #    keyspace_name='%s' and columnfamily_name='%s';""" %
        #    (self.base_keyspace, domain_name))
        result = self._execute(
            """
            SELECT * FROM %s.%s WHERE item_name='%s';
            """ % (self.base_keyspace, domain_name, item_name))
        if result:
            print "Updating existing row"
            self._execute(
                """
                ALTER TABLE %s.%s ADD %s list<text>;                
                """ % (self.base_keyspace, domain_name, attr_name))
            self._execute(
                """
                UPDATE %s.%s SET %s=%s + ['%s'] WHERE item_name='%s';
                """ % (self.base_keyspace, domain_name, attr_name, attr_name,
attr_value, item_name))
        else:
            print "New row..."
            self._execute(
                """
                ALTER TABLE %s.%s ADD %s list<text>;                
                """ % (self.base_keyspace, domain_name, attr_name))
            self._execute(
                """
                INSERT INTO %s.%s (item_name, %s) VALUES ('%s',['%s']);
                """ % (self.base_keyspace, domain_name, attr_name, item_name,
attr_value))

    def get_attributes(self, owner, domain_name, item_name):
        return self._execute(
            """
            SELECT * FROM %s.%s WHERE item_name='%s';
            """ % (self.base_keyspace, domain_name, item_name))
    
    def delete_attribute_value(self, owner, domain_name, item_name, attr_name,
attr_value):
            self._execute(
            """
            UPDATE %s.%s set %s=%s - ['%s'] WHERE item_name='%s';
            """ % (self.base_keyspace, domain_name, attr_name, attr_name,
attr_value, item_name))

    def delete_attribute_all(self, owner, domain_name, item_name, attr_name):
        self._execute(
            """
            DELETE %s FROM %s.%s WHERE item_name='%s';
            """ % (attr_name, self.base_keyspace, domain_name, item_name))
    
driver = CassandraBackend()
driver.create_domain('rackspace', 'user')
driver.list_domains('rackspace')
print driver.domain_metadata('rackspace', 'user')
driver.add_attribute_value('rackspace', 'user', 'sacharya', 'first_name',
'sudarshan')
driver.get_attributes('rackspace', 'user', 'sacharya')
driver.add_attribute_value('rackspace', 'user', 'sacharya', 'last_name',
'acharya')
driver.add_attribute_value('rackspace', 'user', 'sacharya', 'middle_name',
'prasad')
driver.get_attributes('rackspace', 'user', 'sacharya')
driver.delete_attribute_value('rackspace', 'user', 'sacharya', 'middle_name',
'prasad')
driver.get_attributes('rackspace', 'user', 'sacharya')
driver.delete_domain('rackspace', 'user')
driver.list_domains('rackspace')
