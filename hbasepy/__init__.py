import requests
import json
import base64
from urlparse import urlparse

class Client:
	headers = {
	    'Accept': 'application/json',
	    'Content-Type': 'application/json',
	}

	session = requests.session()

	def __init__(self, url):
		parsed = urlparse(url)
		self.url = '%s://%s' % (parsed.scheme, parsed.netloc)


	def version(self):
		r = self.session.get(self.url + '/version/cluster', headers=self.headers)
		return json.loads(r.text)

	def status(self):
		r = self.session.get(self.url + '/status/cluster', headers=self.headers)
		return json.loads(r.text)

	def namespaces(self):
		r = self.session.get(self.url + '/namespaces', headers=self.headers)
		return json.loads(r.text)['Namespace']

	def namespace(self, ns):
		r = self.session.get(self.url + '/namespaces/%s' % ns, headers=self.headers)
		if r.status_code / 100 != 2:
			return
		return json.loads(r.text)

	def namespace_create(self, ns):
		r = self.session.post(self.url + '/namespaces/%s' % ns)
		return r.status_code / 100 == 2

	def namespace_tables(self, ns):
		r = self.session.get(self.url + '/namespaces/%s/tables' % ns, headers=self.headers)
		if r.status_code / 100 != 2:
			return []
		return [table['name'] for table in json.loads(r.text)['table']]

	def namespace_alter(self, ns):
		r = self.session.put(self.url + '/namespaces/%s' % ns, headers=self.headers)
		return r.status_code / 100 == 2

	def namespace_delete(self, ns):
		r = self.session.delete(self.url + '/namespaces/%s' % ns, headers=self.headers)
		return r.status_code / 100 == 2

	def tables(self):
		r = self.session.get(self.url + '/', headers={'Accept': 'application/json'})
		return [table['name'] for table in json.loads(r.text)['table']]

	def table_schema(self, name):
		r = self.session.get(self.url + '/%s/schema' % name, headers=self.headers)
		if r.status_code/100 == 2:
			return json.loads(r.text)

	def table_create(self, name, cf=None):
		if cf == None or len(cf) == 0:
			raise Exception("Need at least one column family")

		data = {'name': name, 'ColumnSchema': cf}

		r = self.session.post(self.url + '/%s/schema' % name, headers=self.headers, json=data)
		return r.status_code/100 == 2

	def table_update(self, name, cf=None):
		if len(cf) == 0:
			raise Exception("Need at least one column family")

		data = {'name': name, 'ColumnSchema': cf}

		r = self.session.put(self.url + '/%s/schema' % name, headers=self.headers, json=data)
		return r.status_code/100 == 2

	def table_delete(self, name):
		r = self.session.delete(self.url + '/%s/schema' % name)
		return r.status_code/100 == 2

	def table_regions(self, name):
		r = self.session.get(self.url + '/%s/regions' % name, headers=self.headers)
		return json.loads(r.text)

	def merge_dicts(self, *dict_args):
	    result = {}
	    for dictionary in dict_args:
	        result.update(dictionary)
	    return result

	def scan_prefix(self, table, prefix, columns=None, batch_size=None):
		encoded_prefix = base64.b64encode(prefix)

		data = {'batch': 1000, 'startRow': encoded_prefix, 'filter': '{"type": "PrefixFilter","value": "%s"}' % encoded_prefix}

		if columns != None:
			data['column'] = [base64.b64encode(c) for c in columns]

		if batch_size != None:
			data['batch'] = batch_size

		r = self.session.put(self.url + '/%s/scanner/' % table, headers=self.headers, json=data)
		if r.status_code / 100 != 2:
			raise Exception("Error creating scanner %s" % r)

		url = r.headers['Location']

		last_row = None
		while True:
			#scroll
			r = self.session.get(url, headers=self.headers)

			#no more docs
			if r.status_code == 204:
				break

			doc = json.loads(r.text)
			total_row = len(doc['Row'])
			for i in range(total_row):
				row = doc['Row'][i]

				#row data
				key = base64.b64decode(row['key'])
				values = {}
				for c in row['Cell']:
					values[base64.b64decode(c['column'])] = base64.b64decode(c['$'])

				full_row = {'key': key, 'values': values}

				#first row of new set
				if last_row != None:
					if last_row['key'] == full_row['key']:
						full_row['values'] = self.merge_dicts(full_row['values'], last_row['values'])
					else:
						#first row is not the continuation of the last set
						yield last_row['key'], last_row['values']

					last_row = None

				#is last row
				if i == total_row - 1:
					last_row = full_row
					break

				#yield full row
				yield full_row['key'], full_row['values']

		if last_row != None:
			yield last_row['key'], last_row['values']

		#print 'Delete'
		r = self.session.delete(url)

	def get(self, table, key, cf=None, ts=None, versions=None):
		url = self.url + '/%s/%s' % (table, key)

		if cf != None:
			url += '/' + cf

		if ts != None:
			url += '/' + ts

		if versions != None:
			url += '?v=%s' % versions

		r = self.session.get(url, headers=self.headers)
		if r.status_code/100 != 2:
			#no result
			return

		row = json.loads(r.text)['Row'][0]

		key = base64.b64decode(row['key'])
		values = {}
		for c in row['Cell']:
			values[base64.b64decode(c['column'])] = base64.b64decode(c['$'])

		return key, values

	def get_many(self, table, keys):
		if len(keys) == 0:
			return

		url = self.url + '/%s/multiget?' % table
		for key in keys:
			url += 'row=%s&' % key


		r = self.session.get(url, headers=self.headers)
		if r.status_code/100 != 2:
			return

		doc = json.loads(r.text)
		for row in doc['Row']:
			#row data
			key = base64.b64decode(row['key'])
			values = {}
			for c in row['Cell']:
				values[base64.b64decode(c['column'])] = base64.b64decode(c['$'])

			yield key, values


	def put(self, table, values):
		if len(values) == 0:
			return

		rows = []
		for val in values:
			row = {'key': base64.b64encode(val['key']), 'Cell': []}
			for col, v in val['values'].iteritems():
				row['Cell'].append({'column': base64.b64encode(col), '$': base64.b64encode(v)})

			rows.append(row)

		data = {"Row": rows}

		r = self.session.put(self.url + '/%s/1' % table, headers=self.headers, json=data)
		return r.status_code/100 == 2