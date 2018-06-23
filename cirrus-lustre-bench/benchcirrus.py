#! /usr/bin/env python3
'''
Benchmarking I/O performance on Cirrus Lustre
'''

import os, sys, configparser
import numpy as np
from mpi4py import MPI

# Configurations
config = configparser.ConfigParser()
config_file = 'config.ini'
config.read(config_file)
config_bench = config['BENCH']

# MPI envs
RANK_MASTER = 0
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
processor_name = MPI.Get_processor_name()

def bench_cirrus_get_with_posix_io():
	'''
	Benchmarking cirrus get with multiple access on a single file

	Return:
	 int[][] : source stream
	'''
	print('Rank {0} of {1}. Processor[{2}]'.format(rank, size, processor_name))
	
	# Matrics 
	max_read = 0
	min_read = sys.maxsize
	avg_read = 0

	for _ in range(0, int(config_bench['repeat_time'])):
		comm.Barrier()
		start = MPI.Wtime()
		with open(config_bench['source_file'], 'r') as f:
			data = f.read()
		end = MPI.Wtime()
		comm.Barrier()

		# Metrics
		read_time = np.zeros(1)
		max_read_time = np.zeros(1)
		min_read_time = np.zeros(1)
		avg_read_time = np.zeros(1)
		read_time[0] = end - start
		comm.Reduce(read_time, max_read_time, MPI.MAX)
		comm.Reduce(read_time, min_read_time, MPI.MIN)
		comm.Reduce(read_time, avg_read_time, MPI.SUM)

		if RANK_MASTER == rank:
			max_read_time[0] = round(max_read_time[0], 3)
			min_read_time[0] = round(min_read_time[0], 3)
			avg_read_time[0] = round(avg_read_time[0] / size, 3)

			max_read = max(max_read_time[0], max_read)
			min_read = min(min_read_time[0], min_read)
			avg_read += avg_read_time[0]
	
	if RANK_MASTER == rank:
		avg_read = round(avg_read / int(config_bench['repeat_time']), 3)
		print('-------- Single file, Cirrus Lustre --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} KiB inputs on {1} processes'.format(os.path.getsize(config_bench['source_file']) >> 10, size))
		print('Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))

	return data


if __name__ == '__main__':
	bench_cirrus_get_with_posix_io()