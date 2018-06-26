#! /usr/bin/env python3
' Benchmarking Azure blob performance for HPC purpose'

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from azure.storage.blob import BlockBlobService
from common import common

def bench_blob_get_with_single_blob_single_container():
	'''
	Benchmarking Blob get with multiple access on a single blob within a single container

	Return:
		type[] : source stream
	'''
	# Configurations
	config_file = 'config.ini'
	config_azure = common.get_config(config_file, 'AZURE')
	config_bench = common.get_config(config_file, 'BENCH')

	# MPI envs
	rank, size, _ = common.get_mpi_env()

	# Azure
	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	blob_size = block_blob_service.get_blob_properties(config_azure['source_container_name'], config_azure['source_blob_name']).properties.content_length

	# Metrics
	max_read, min_read, avg_read = common.init_bench_metrics()

	for _ in range(0, int(config_bench['repeat_time'])):
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		data = block_blob_service.get_blob_to_text(config_azure['source_container_name'], config_azure['source_blob_name'])
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		# Metrics for current iteration
		max_read_time, min_read_time, avg_read_time = common.collect_get_bench_metrics(end - start)

		# General metrics
		max_read = max(max_read_time, max_read)
		min_read = min(min_read_time, min_read)
		avg_read += avg_read_time
	
	avg_read = round(avg_read / int(config_bench['repeat_time']), 3)

	if 0 == rank:
		print('-------- Single Blob, Single Container --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} KiB Inputs on {1} Processes --------'.format(round(blob_size/ 1024, 3), size))
		print('Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))
	
	return data

if __name__ == '__main__':
	bench_blob_get_with_single_blob_single_container()