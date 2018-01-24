import csv
import argparse
import os
import re
from glob import glob

SHARD_REGEX = re.compile(r'(.+)\.(\d{3})')

def set_shard_name(file_name, shard_number):
	fn, ext = os.path.splitext(file_name)
	return '{}.{:03d}{}'.format(fn, shard_number, ext)

def get_shard_name(file_name):
	fn, ext = os.path.splitext(file_name)
	match = SHARD_REGEX.match(fn)
	if match:
		return dict(file_name='{}{}'.format(match.group(1), ext), shard_number=int(match.group(2)))

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

class Writer(object):
	def __init__(self, file_name):
		self.file_name = file_name
		self.shard_numbers = []
		self.shards = {}
		self.field_names = []

		self.read_shards()
		self.writer = csv.DictWriter(open(file_name, 'w'), fieldnames=self.field_names)

	def read_shards(self):
		fn, ext = os.path.splitext(self.file_name)
		file_list = glob('{}.???{}'.format(fn, ext))

		for file_name in file_list:
			shard = get_shard_name(file_name)
			if shard:
				self.shard_numbers.append(shard['shard_number'])
				self.shards[shard['shard_number']] = csv.DictReader(open(file_name, 'r'))
				for key in self.shards[shard['shard_number']].fieldnames:
					if key not in self.field_names:
						self.field_names.append(key)

	def write(self):
		self.writer.writeheader()
		for s in sorted(self.shard_numbers):
			 for row in self.shards[s]:
			 	self.writer.writerow(row)

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

def shard(args):
	r = Reader(args.filename, max_rows=args.n)
	while True:
		r.next()

def unshard(args):
	w = Writer(args.filename)
	w.write()

if __name__ == '__main__':
	import argparse
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	shard_parser = subparsers.add_parser('shard')
	shard_parser.set_defaults(func=shard)
	unshard_parser = subparsers.add_parser('unshard')
	unshard_parser.set_defaults(func=unshard)

	shard_parser.add_argument('-n', help='Number of lines to keep in a shard', type=int, default=10000)
	shard_parser.add_argument('filename', help='CSV file to be sharded')
	unshard_parser.add_argument('filename', help='CSV file to be unsharded')

	args = parser.parse_args()
	args.func(args)