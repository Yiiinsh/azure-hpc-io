#! /usr/bin/env python3
'''
Benchmarking I/O performance for HPC purpose
'''

import configparser
from mpi4py import MPI
from common import common
from cirrus_lustre_bench import benchcirrus
from azure_blob_bench import benchblob
from azure_file_bench import benchfile
from tool.bench_azure_blob import AzureBlobBench

def bench():
	# Configurations
	config = configparser.ConfigParser()
	config.read('config.ini')
	config_bench = config['BENCH']
	config_azure = config['AZURE']

	# MPI envs
	if bool(config_bench['show_mpi_env']):
		rank, size, proc_name = common.get_mpi_env()
		print('Rank {0} of {1}. Proc name:{2}'.format(rank, size, proc_name))
		print()

	# Bench tools
	bench_items = config_bench['bench_items']
	bench_targets = config_bench['bench_targets']
	repeat_times = int(config_bench['repeat_time'])

	# Benchmarking
	if bench_targets == 'azure_blob':
		azure_blob_bench = AzureBlobBench(config_azure['account_name'], config_azure['account_key'], [config_azure['input_container_name']])

		if bench_items == 'input':
			for _ in range(0, repeat_times):
				max_time, min_time, avg_time = azure_blob_bench.bench_inputs_with_single_blob(config_azure['input_container_name'], config_azure['input_blob_name'])
				if 0 == MPI.COMM_WORLD.Get_rank():
					print(max_time, min_time, avg_time)
	
if __name__ == '__main__':
	bench()
	