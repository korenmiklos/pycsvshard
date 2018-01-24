import csv
import argparse
import os

def set_shard_name(file_name, shard_number):
	fn, ext = os.path.splitext(file_name)
	return '{}.{:03d}{}'.format(fn, shard_number, ext)

class Buffer(object):
	def __init__(self, file_name, shard_number, field_names, max_rows):
		self.csv_writer = csv.DictWriter(open(set_shard_name(file_name, shard_number), 'w'), fieldnames=field_names)
		self.csv_writer.writeheader()
		self.rows = 0
		self.max_rows = max_rows

	def write(self, row):
		if self.rows <= self.max_rows:
			self.csv_writer.writerow(row)
			self.rows += 1
		else:
			raise StopIteration

class Reader(object):
	def __init__(self, file_name, max_rows=100000):
		self.file_name = file_name
		self.csv_reader = csv.DictReader(open(self.file_name, 'r'))
		self.field_names = self.csv_reader.fieldnames
		self.max_rows = max_rows
		self.shard_number = 1
		self.buffer = Buffer(self.file_name, self.shard_number, self.field_names, max_rows=self.max_rows)

	def read(self):
		return next(self.csv_reader)

	def next(self):
		row = self.read()
		try:
			self.buffer.write(row)
		except StopIteration:
			self.shard_number += 1
			self.buffer = Buffer(self.file_name, self.shard_number, self.field_names, max_rows=self.max_rows)
			self.buffer.write(row)

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-n', help='Number of lines to keep in a shard', type=int, default=10000)
	parser.add_argument('filename', help='CSV file to be sharded')
	args = vars(parser.parse_args())

	r = Reader(args['filename'], max_rows=args['n'])
	while True:
		r.next()
