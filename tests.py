import hbasepy

def test_flow():
	c = hbasepy.Client('http://10.100.1.212:8070')
	c.table_delete('test')

	r = c.table_create('test', cf=[{'name': 'yo', 'maxVersions': 1}])
	assert r, 'Create table failed'

	r = c.table_schema('test')
	assert 'ColumnSchema' in r, 'get table schema failed'

	keys = ['1:1', '2:1']
	r = c.put('test', [{'key': '1:1', 'values': {'yo:lll': '1111s', 'yo:2': '22222'}}, {'key': '1:2', 'values': {'yo:qqq': 'wwrt'}}])
	assert r, 'Insert data failed'

	k, v = c.get('test', '1:1', cf='yo:2')
	assert k == '1:1', 'Get row failed'
	assert len(v) == 1, 'Select specific column failed'

	k, v = c.get('test', '1:2')
	assert k == '1:2', 'Get row failed'

	for k, v in c.scan_prefix('test', '1:', columns=['yo:2']):
		assert k == '1:1', 'Get row failed'
		assert len(v) == 1, 'Select specific column failed'

	rows = list(c.scan_prefix('test', '1:', columns=['yo']))
	assert len(rows) == 2, 'Not all row scanned'

	rows = list(c.get_many('test', ['1:1', '1:2']))
	assert len(rows) == 2, 'Multiget failed'

	r = c.table_delete('test')
	assert r, 'Delete table failed'