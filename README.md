# hbasepy
A simple hbase REST client. 

##Features
- Cluster version/status
- Namespace CRUD
- Table CRUD
- Bulk inserts
- Scan
- Prefix scan
- Multiget

##Install
```
pip install hbasepy
```

##Usage
```
>>> import hbasepy
>>> c = hbasepy.Client('http://domain.com:8070')
```

##Cluster
###Versions
```
>>> c.version()
u'1.2.2'
```

###Status
```
>>> c.status().keys()
[u'regions', u'DeadNodes', u'requests', u'LiveNodes', u'averageLoad']
```

###Info
```
>>> c.info()
{u'OS': u'Linux 4.4.19-29.55.amzn1.x86_64 amd64', u'JVM': u'Oracle Corporation 1.8.0_101-25.101-b13', u'REST': u'0.0.3', u'Jersey': u'1.9', u'Server': u'jetty/6.1.26'}
```

##Namespace
###List
```
>>> c.namespaces()
[u'default', u'hbase']
```

###Get
```
>>> c.namespace('testns')
{u'properties': None}
```

###Create
```
>>> c.namespace_create('testns')
True
```

###Tables
```
>>> c.namespace_tables('testns')
[]
```

###Alter
```
>>> c.namespace_alter('testns')
True
```

###Delete
```
>>> c.namespace_delete('testns')
True
```

##Table
###List
```
>>> c.tables()
[u'test']
```

###Get
```
>>> c.table_schema('test')
{u'ColumnSchema': [{u'BLOCKCACHE': u'true', u'name': u'col', u'VERSIONS': u'1', u'KEEP_DELETED_CELLS': u'FALSE', u'maxVersions': u'1', u'BLOCKSIZE': u'65536', u'MIN_VERSIONS': u'0', u'DATA_BLOCK_ENCODING': u'NONE', u'REPLICATION_SCOPE': u'0', u'TTL': u'2147483647', u'IN_MEMORY': u'false', u'BLOOMFILTER': u'ROW', u'COMPRESSION': u'NONE'}], u'name': u'test', u'IS_META': u'false'}
```

###Create
```
>>> c.table_create('test', [{'name': 'col', 'maxVersions': 1}])
True
```

###Update
Replace the table definition. You have to specify existing column you want to keep. If you don't they are dropped.
```
>>> c.table_update('test', [{'name': 'col', 'maxVersions': 1}, {'name': 'col2'}])
True
```

###Regions
```
>>> c.table_regions('test')
{u'Region': [{u'startKey': u'', u'endKey': u'', u'id': 1478813628562, u'name': u'test,,1478813628562.c905e2fcc2e543bc139bbd5796bf3de3.', u'location': u'ip-X-X-X-X.us-west-2.compute.internal:16020'}], u'name': u'test'}
```

###Delete
```
>>> c.table_delete('test')
True
```

##Put data
Values is format `'c:f': 'val'` where `val` must be an instance of `basestring` to be base64 encoded.
```
c.put('test', [{'key': '1:1', 'values': {'yo:lll': '1111s', 'yo:2': '22222'}}, {'key': '1:2', 'values': {'yo:qqq': 'wwrt'}}])
True
```

##Scan
```
>>> for k, v in c.scan('test'):
...   print k
...
1:1
1:2

>>> for k, v in c.scan('test', prefix='1:'):
...   print k
...
1:1
1:2

>>> for k, v in c.scan('test', prefix='1:', columns=['yo:2']):
...   print k
...
1:1
```

##Get row
```
>>> c.get('test', '1:1')
('1:1', {'yo:lll': '1111s', 'yo:2': '22222'})

>>> c.get('test', '1:1', cf='yo:2')
('1:1', {'yo:2': '22222'})

>>> c.get('test', '1:2', include_timestamp=True)
('1:2', {'yo:qqq': ('wwrt', 1478913898453)})
```

##Get many
```
>>> for k, v in c.get_many('test', ['1:1', '1:2']):
...   print k, v
...
1:1 {'yo:lll': '1111s', 'yo:2': '22222'}
1:2 {'yo:qqq': 'wwrt'}
```

##Run tests
```
$ py.test tests.py
```
