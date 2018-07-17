#! /usr/bin/env python3
'''
Common tools for azure-hpc-io benchmarking
'''

import sys, configparser
import numpy as np
from mpi4py import MPI

def collect_bench_metrics(time, precision = 3):
	'''
	Clollect input benchmarking metrics

	param:
	 time: elapsed time for a single reading
	
	return:
	 max_time: maximum operation time
	 min_time: minimum operation time
	 avg_time: average operation time
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

	max_read_time[0] = round(max_read_time[0], precision)
	min_read_time[0] = round(min_read_time[0], precision)
	avg_read_time[0] = round(avg_read_time[0] / MPI.COMM_WORLD.Get_size(), precision)

	return max_read_time[0], min_read_time[0], avg_read_time[0]

def get_mpi_env():
	'''
	Get MPI environmental parameters.

	return:
	 [int]rank : rank of current process
	 [int]size : size of processes used in MPI_COMM_WORLD
	 [str]processor_name : current processor name
	'''
	return MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size(), MPI.Get_processor_name()