#! /usr/bin/env python3
'''
Benchmarking I/O performance on Cirrus Lustre
'''

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from common import common

def bench_cirrus_get_with_posix_io():
	'''
	Benchmarking cirrus get with multiple access on a single file

	Return:
	 [str]data : source stream in text format
	'''
	# Configurations
	config_file = 'config.ini'
	config_bench = common.get_config(config_file, 'BENCH')

	# MPI envs
	rank, size, _ = common.get_mpi_env()
	
	# Matrics 
	max_read, min_read, avg_read = common.init_bench_metrics()

	for _ in range(0, int(config_bench['repeat_time'])):
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		with open(config_bench['source_file'], 'r') as f:
			data = f.read()
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		# Metrics for current iteration
		max_read_time, min_read_time, avg_read_time = common.collect_get_bench_metrics(end - start)

		# General metrics
		max_read = max(max_read_time, max_read)
		min_read = min(min_read_time, min_read)
		avg_read += avg_read_time
	
	if 0 == rank:
		avg_read = round(avg_read / int(config_bench['repeat_time']), 3)
		print('-------- Single file, Cirrus Lustre --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} KiB inputs on {1} processes'.format(os.path.getsize(config_bench['source_file']) >> 10, size))
		print('Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))

	return data


if __name__ == '__main__':
	bench_cirrus_get_with_posix_io()