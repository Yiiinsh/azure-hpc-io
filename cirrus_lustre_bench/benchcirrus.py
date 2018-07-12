#! /usr/bin/env python3
'''
Benchmarking I/O performance on Cirrus Lustre
'''

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from common import common

# Configurations
config_file = 'config.ini'
config_bench = common.get_config(config_file, 'BENCH')
config_azure = common.get_config(config_file, 'AZURE')

# MPI envs
rank, size, _ = common.get_mpi_env()

def bench_cirrus_get_with_posix_io():
	'''
	Benchmarking cirrus get with multiple access on a single file

	Return:
	 [str]data : source stream in text format
	'''
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

def bench_cirrus_write_with_single_file_multiple_writers():
	'''
	Benchmark cirrus write with multiple access on a single file

	'''
	if 0 == rank:
		print('-------- Cirrus File --------')
		print('-------- Single File, Multiple Writers --------')
		print('-------- Doesn\'t support for now --------')
		print()

def bench_cirrus_write_with_multiple_files_multiple_writers():
	'''
	Benchmark cirrus write with multiple access on a multiple files

	'''
	# Data prepare
	write_size_per_rank = int(config_bench['write_size_per_rank']) << 20 # in bytes
	data = bytes( rank for i in range(0, write_size_per_rank) )
	file_name = config_azure['output_file_name'] + '{:0>5}'.format(rank)

	# Metrics
	avg_write_per_rank = 0 # Average write time for each rank putting their own files
	avg_write_all_ranks = 0 # Average write time for all ranks to finish writing and reach the barrier

	for _ in range(0, int(config_bench['repeat_time'])):
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()

		with open(file_name, 'wb') as f:
			f.write(data)

		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		all_write_end = MPI.Wtime()

		_, _, avg_write_time = common.collect_get_bench_metrics(end - start)
		avg_write_per_rank += avg_write_time
		avg_write_all_ranks += all_write_end - start
	
	# Results
	repeat_time = int(config_bench['repeat_time'])
	avg_write_per_rank = round(avg_write_per_rank / repeat_time, 3)
	avg_write_all_ranks = round(avg_write_all_ranks / repeat_time, 3)

	if 0 == rank:
		file_size = (write_size_per_rank >> 20) * size # in MiB
		bandwidth = round(file_size / avg_write_all_ranks, 3)
		propotion_write_per_rank = avg_write_per_rank / avg_write_all_ranks

		print('-------- Cirrus File --------')
		print('-------- Multiple Files, Multiple Writers --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Outputs on {1} Processes, {2} MiB Each --------'.format(file_size, size, write_size_per_rank >> 20))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print('Propotion: Write per process: {:.2%}'.format(propotion_write_per_rank))
		print()