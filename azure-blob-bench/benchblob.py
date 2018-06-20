#! /usr/bin/env python3 

' Benchmarking Azure blob performance for HPC purpose'

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from azure.storage.blob import BlockBlobService

# Configurations
config = configparser.ConfigParser()
config_file = 'config.ini'
config.read(config_file)
config_azure = config['AZURE']
config_bench = config['BENCH']

# MPI envs
RANK_MASTER = 0
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
processor_name = MPI.Get_processor_name()

def bench_blob():
	print('Rank {0} of {1}. Processor[{2}]'.format(rank, size, processor_name))

	bench_blob_get_with_single_blob_single_container()


def bench_blob_get_with_single_blob_single_container():
	'''
	Benchmarking Blob get with multiple access on a single blob within a single container

	Return:
		type[] : source stream
	'''

	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	blob_size = block_blob_service.get_blob_properties(config_azure['source_container_name'], config_azure['source_blob_name']).properties.content_length

	max_read = 0
	min_read = sys.maxsize
	avg_read = 0
	for _ in range(0, int(config_bench['repeat_time'])):
		comm.Barrier()
		start = MPI.Wtime()
		data = block_blob_service.get_blob_to_bytes(config_azure['source_container_name'], config_azure['source_blob_name'])
		end = MPI.Wtime()
		comm.Barrier()

		# Metrics
		read_time = np.zeros(1)
		max_read_time = np.zeros(1)
		min_read_time = np.zeros(1)
		avg_read_time = np.zeros(1)
		read_time[0] = end - start
		comm.Reduce(read_time, max_read_time, MPI.MAX, RANK_MASTER)
		comm.Reduce(read_time, min_read_time, MPI.MIN, RANK_MASTER)
		comm.Reduce(read_time, avg_read_time, MPI.SUM, RANK_MASTER)

		if RANK_MASTER == rank:
			max_read_time[0] = round(max_read_time[0], 3)
			min_read_time[0] = round(min_read_time[0], 3)
			avg_read_time[0] = round(avg_read_time[0] / size, 3)

			max_read = max(max_read_time[0], max_read)
			min_read = min(min_read_time[0], min_read)
			avg_read += avg_read_time[0]

	if RANK_MASTER == rank:
		avg_read = round(avg_read / int(config_bench['repeat_time']), 3)

		print('-------- Single Blob, Single Container --------')
		print('-------- {0} KiB Inputs on {1} Processes --------'.format(round(blob_size/ 1024, 3), size))
		print('Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))
	
	return data


def bench_blob_get_with_multiple_blob_single_container():
	pass

def bench_blob_get_with_multiple_blob_multiple_container():
	pass

def bench_blob_get_with_master_download_and_distribute():
	pass

if __name__ == '__main__':
	bench_blob()