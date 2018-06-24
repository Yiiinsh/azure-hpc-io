#! /usr/bin/env python3
'''
Common tools for azure-hpc-io benchmarking
'''
import sys, configparser
import numpy as np
from mpi4py import MPI

def get_mpi_env():
	'''
	Get MPI environmental parameters.

	return:
	 [int]rank : rank of current process
	 [int]size : size of processes used in MPI_COMM_WORLD
	 [str]processor_name : current processor name
	'''
	return MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size(), MPI.Get_processor_name()

def get_config(config_file, config_section):
	'''
	Get configuration section from specified configuration file.

	return:
	 [section]config : specified configuration section
	'''
	config = configparser.ConfigParser()
	config.read(config_file)

	if not config.has_section(config_section):
		raise AttributeError('Connot find config section')
	
	return config[config_section]

def init_bench_metrics():
	'''
	Initialize benchmarking metrics.

	return:
	 [0]max_read : metric recording maximum read time.
	 [sys.max]min_read : metric recording minimal read time.
	 [0]avg_read : metric recording average read time.
	'''
	return 0, sys.maxsize, 0

def collect_get_bench_metrics(time):
	'''
	Collect benchmarking metrics 

	param:
	 [float]time : read time for current process
	 [int]proc_size : size of processors

	return:
	 [float]max_read : maximum read time for all process
	 [float]min_read : minimal read time for all processes
	 [float]avg_read : average read time for all processes
	'''
	# Metrics
	read_time = np.zeros(1)
	max_read_time = np.zeros(1)
	min_read_time = np.zeros(1)
	avg_read_time = np.zeros(1)
	read_time[0] = time

	MPI.COMM_WORLD.Reduce(read_time, max_read_time, MPI.MAX)
	MPI.COMM_WORLD.Reduce(read_time, min_read_time, MPI.MIN)
	MPI.COMM_WORLD.Reduce(read_time, avg_read_time, MPI.SUM)

	max_read_time[0] = round(max_read_time[0], 3)
	min_read_time[0] = round(min_read_time[0], 3)
	avg_read_time[0] = round(avg_read_time[0] / MPI.COMM_WORLD.Get_size(), 3)

	return max_read_time[0], min_read_time[0], avg_read_time[0]

if __name__ == '__main__':
	rank, size, proc_name = get_mpi_env()
	print('Rank {0} of {1}. Processor name {2}'.format(rank, size, proc_name))

	max_read, min_read, avg_read = init_bench_metrics()
	print(max_read, min_read, avg_read)
