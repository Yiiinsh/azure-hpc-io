#! /usr/bin/env python3
'''
Benchmarking I/O performance for HPC purpose
'''

import configparser
from mpi4py import MPI
from common import common
from tool.bench_azure_blob import AzureBlobBench
from tool.bench_azure_file import AzureFileBench
from tool.bench_cirrus_lustre import CirrusLustreBench

def bench():
	# Configurations
	config = configparser.ConfigParser()
	config.read('config.ini')
	config_bench = config['BENCH']
	config_azure = config['AZURE']

	# MPI envs
	rank, size, proc_name = common.get_mpi_env()
	if bool(config_bench['show_mpi_env']):
		print('Rank {0} of {1}. Proc name:{2}'.format(rank, size, proc_name))
		print()

	# Bench tools
	bench_items = config_bench['bench_items']
	bench_targets = config_bench['bench_targets']
	repeat_times = int(config_bench['repeat_time'])
	bench_pattern = config_bench['bench_pattern']

	# Benchmarking
	if 0 == rank:
		print('Bench Target: {0}, Bench Item: {1}'.format(bench_targets, bench_items))
	if bench_targets == 'azure_blob':
		azure_blob_bench = AzureBlobBench(config_azure['account_name'], config_azure['account_key'], [config_azure['input_container_name']])

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_blob_bench.bench_inputs_with_single_blob(config_azure['input_container_name'], config_azure['input_blob_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_blob_bench.bench_inputs_with_multiple_blobs(config_azure['input_container_name'], config_azure['input_blob_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
	elif bench_targets == 'azure_file':
		azure_file_bench = AzureFileBench(config_azure['account_name'], config_azure['account_key'], [config_azure['input_share_name']])

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_file_bench.bench_inputs_with_single_file(config_azure['input_share_name'], config_azure['input_directory_name'], config_azure['input_file_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_file_bench.bench_inputs_with_multiple_files(config_azure['input_share_name'], config_azure['input_directory_name'], config_azure['input_file_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
	elif bench_targets == 'cirrus_lustre':
		cirrus_lustre_bench = CirrusLustreBench()

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = cirrus_lustre_bench.bench_inputs_with_single_file(config_azure['input_file_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = cirrus_lustre_bench.bench_inputs_with_multiple_files(config_azure['input_file_name'])
					if 0 == rank:
						print(max_time, min_time, avg_time)
	
if __name__ == '__main__':
	bench()
	