from cassandra.cluster import Cluster

server_list = ['cassandra1','cassandra2','cassandra3']

cluster = Cluster(server_list)
session = cluster.connect()

def execute(query):
    print "====================================================="
    print query
    result = session.execute(query) 
    print result
    return result       

# Check existing keyspaces.
result = execute(
    """
    SELECT * from system.schema_keyspaces WHERE keyspace_name='web';
    """)

# Delete the keyspace
if result:
    execute(
    """
    DROP KEYSPACE web;
    """)

# Create a keyspace
execute(
    """
    CREATE KEYSPACE web
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

execute(
    """
    CREATE KEYSPACE IF NOT EXISTS web
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

execute(
    """
    CREATE TABLE IF NOT EXISTS web.author(
        username text,
        first_name text,
        last_name text,
        skills set<text>,
        PRIMARY KEY(username)
    );
    """)

execute(
    """
    INSERT INTO web.author (username, first_name, last_name, skills) VALUES
    ('jbrown','johnny','brown', {'Java', 'Python'});
    """)

execute(
    """
    UPDATE web.author SET skills = skills + {'Bash'} WHERE username='jbrown';
    """)

execute(
    """
    SELECT * FROM web.author WHERE username='jbrown';
    """)

# if I want to query by a column other than a primar key, index it first
execute("CREATE INDEX on web.author(first_name)")
execute(
    """
    SELECT * FROM web.author WHERE first_name='johnny';
    """)

execute(
    """
    DELETE skills FROM web.author WHERE username='jbrown';
    """)

execute(
    """
    UPDATE web.author SET skills = {} WHERE username='jbrown';
    """)

execute(
    """
    ALTER TABLE web.author ADD phone_numbers list<text>;
    """)

execute(
    """
    UPDATE web.author SET phone_numbers = ['123456789', '987654321',
'123456789'] +
    phone_numbers WHERE username='jbrown';
    """)    

execute(
    """
    SELECT * FROM web.author WHERE username='jbrown';
    """)

execute(
    """
    ALTER TABLE web.author ADD skill_levels map<text, int>;
    """)

execute(
    """
    UPDATE web.author SET skill_levels = {'Java':8, 'Python':5} WHERE
username='jbrown';
    """)

execute(
    """
    SELECT * FROM web.author WHERE username='jbrown';
    """)

execute(
    """
    DELETE FROM web.author WHERE username='jbrown';
    """)

execute(
    """
    SELECT * from system.schema_columnfamilies where keyspace_name='web' and
columnfamily_name='author';
    """)

cluster.shutdown()
