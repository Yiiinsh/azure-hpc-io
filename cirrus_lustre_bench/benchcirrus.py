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
	
	# Metrics 
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

		# Global metrics
		max_read = max(max_read_time, max_read)
		min_read = min(min_read_time, min_read)
		avg_read += avg_read_time
	
	avg_read = round(avg_read / int(config_bench['repeat_time']), 3)

	if 0 == rank:
		file_size = os.path.getsize(config_bench['source_file']) >> 20
		bandwidth = round(file_size / avg_read, 3)
		print('-------- Cirrus Lustre --------')
		print('-------- Single file, Multiple Reader --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB inputs on {1} processes'.format(file_size, size))
		print('Latency: Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print()

	return data