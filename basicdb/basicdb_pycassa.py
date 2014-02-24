from pycassa import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import SIMPLE_STRATEGY
from pycassa.system_manager import SystemManager
from pycassa.system_manager import UTF8_TYPE

import time
import uuid


class CassandraBackend(object):
    def __init__(self, base_keyspace='basicdb'):
    self.base_keyspace = base_keyspace
        server_list = ['cassandra1:9160', 'cassandra2:9160', 'cassandra3:9160']
        self.sys = SystemManager(server_list[0])
        if base_keyspace in self.sys.list_keyspaces():
            self.sys.drop_keyspace(base_keyspace)
    self.sys.create_keyspace(base_keyspace, SIMPLE_STRATEGY,
{'replication_factor': '1'})
    self.pool = ConnectionPool(base_keyspace, server_list=server_list)

    def _owner_cf(self, owner):
        return ColumnFamily(self.pool, owner)
    
    def _domain_cf(self, owner, domain_name):
        owner_cf = self._owner_cf(owner)
        domains = owner_cf.get(domain_name)
    return ColumnFamily(self.pool, domain_name)

    def create_domain(self, owner, domain_name):
        self.sys.create_column_family(self.base_keyspace, owner,
comparator_type=UTF8_TYPE)
        owner_cf = self._owner_cf(owner)
        domain_name_uuid = str(uuid.uuid4())        
        owner_cf.insert(domain_name, {'domain': domain_name_uuid})
    self.sys.create_column_family(self.base_keyspace, domain_name,
comparator_type=UTF8_TYPE)

    def delete_domain(self, owner, domain_name):
        owner_cf = self._owner_cf(owner)
        domains = owner_cf.get(domain_name)
        owner_cf.remove(domain_name)

    def list_domains(self, owner):
        owner_cf = self._owner_cf(owner)
    return list(owner_cf.get_range())

    def domain_metadata(self, owner, domain_name):
        keys = self._domain_cf(owner, domain_name).get_range()
    return {"ItemCount": len(list(keys)),
            "ItemNamesSizeBytes": '120',
            "AttributeNameCount": '12',
            "AttributeNamesSizeBytes": '120',
            "AttributeValueCount": '120',
            "AttributeValuesSizeBytes": '100020',
            "Timestamp": str(int(time.time()))}

    def add_attribute_value(self, owner, domain_name, item_name, attr_name,
attr_value):
    domain_cf = self._domain_cf(owner, domain_name)
    domain_cf.insert(item_name, {attr_name: attr_value})

    def get_attributes(self, owner, domain_name, item_name):
    domain_cf = self._domain_cf(owner, domain_name)
    return domain_cf.get(item_name)     
    
    def delete_attribute_value(self, owner, domain_name, item_name, attr_name,
attr_value):
    domain_cf = self._domain_cf(owner, domain_name)
    domain_cf.remove(item_name, columns=[attr_name])

    def delete_attribute_all(self, owner, domain_name, item_name, attr_name):
    domain_cf = self._domain_cf(owner, domain_name)
    domain_cf.remove(item_name, columns=[attr_name])
    
driver = CassandraBackend()
print 'Creating domain'
driver.create_domain('1000', 'user')
time.sleep(5)
print 'Listing domains'
print driver.list_domains('1000')
time.sleep(5)
print 'Domain Metadata'
print driver.domain_metadata('1000', 'user')
time.sleep(5)
print 'Add attribute value'
driver.add_attribute_value('1000', 'user', 'kvile', 'first_name', 'kurt')
time.sleep(5)
print 'Get attributes'
print driver.get_attributes('1000', 'user', 'kvile')
time.sleep(5)
print 'Add attribute value'
driver.add_attribute_value('1000', 'user', 'kvile', 'last_name', 'vile')
time.sleep(5)
print 'Delete attribute value'
driver.delete_attribute_value('1000', 'user', 'kurt', 'first_name', 'vile')
time.sleep(5)
print 'Get attributes'
print driver.get_attributes('1000', 'user', 'kvile')
time.sleep(5)
print 'Delete domain'
driver.delete_domain('1000', 'user')
time.sleep(5)
print 'List domains'
print driver.list_domains('1000')
