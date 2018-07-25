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

	# Bench specifications
	bench_items = config_bench['bench_items']
	bench_targets = config_bench['bench_targets']
	repeat_times = int(config_bench['repeat_time'])
	bench_pattern = config_bench['bench_pattern']

	# Bench infos
	account_name = config_azure['account_name']
	account_key = config_azure['account_key']
	input_container_name = config_azure['input_container_name']
	input_share_name = config_azure['input_share_name']
	input_directory_name = config_azure['input_directory_name']
	input_blob_name = config_azure['input_blob_name']
	input_file_name = config_azure['input_file_name']
	output_container_name = config_azure['output_container_name']
	output_share_name = config_azure['output_share_name']
	output_directory_name = config_azure['output_directory_name']
	output_blob_name = config_azure['output_blob_name']
	output_file_name = config_azure['output_file_name']
	output_per_rank = int(config_bench['output_per_rank'])

	MPI.COMM_WORLD.Barrier()

	# Benchmarking
	if 0 == rank:
		print('Bench Target: {0}, Bench Item: {1}, Bench Pattern:{2}, Bench repeat {3} times'.format(bench_targets, bench_items, bench_pattern, repeat_times))
	
	if bench_targets == 'azure_blob':
		azure_blob_bench = AzureBlobBench(account_name, account_key, [input_container_name])

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_blob_bench.bench_inputs_with_single_block_blob(input_container_name, input_blob_name)
					__print_metrics(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_blob_bench.bench_inputs_with_multiple_block_blobs(input_container_name, input_blob_name)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		elif bench_items == 'output':
			if bench_pattern == 'SFMW':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time, post_time = azure_blob_bench.bench_outputs_with_single_block_blob(output_container_name, output_blob_name, output_per_rank)
					__print_metrics(max_time, min_time, avg_time, post_time)
			elif bench_pattern == 'MFMW':
				data = bytes(rank for i in range(0, output_per_rank << 20))
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_blob_bench.bench_outputs_with_multiple_blockblob(output_container_name, output_blob_name, output_per_rank, data = data)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		else:
			raise ValueError('Unknown item ' + bench_items)
	elif bench_targets == 'azure_file':
		azure_file_bench = AzureFileBench(account_name, account_key, [input_share_name])

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_file_bench.bench_inputs_with_single_file(input_share_name, input_directory_name, input_file_name)
					__print_metrics(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_file_bench.bench_inputs_with_multiple_files(input_share_name, input_directory_name, input_file_name)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		elif bench_items == 'output':
			if bench_pattern == 'SFMW':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time, pre_time = azure_file_bench.bench_outputs_with_single_file(output_share_name, output_directory_name, output_file_name, output_per_rank)
					__print_metrics(max_time, min_time, avg_time, pre_time)
			elif bench_pattern == 'MFMW':
				data = bytes(rank for i in range(0, output_per_rank << 20))
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = azure_file_bench.bench_outputs_with_multiple_files(output_share_name, output_directory_name, output_file_name, output_per_rank, data = data)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		else:
			raise ValueError('Unknown item ' + bench_items)
	elif bench_targets == 'cirrus_lustre':
		cirrus_lustre_bench = CirrusLustreBench()

		if bench_items == 'input':
			if bench_pattern == 'SFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = cirrus_lustre_bench.bench_inputs_with_single_file(input_file_name)
					__print_metrics(max_time, min_time, avg_time)
			elif bench_pattern == 'MFMR':
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = cirrus_lustre_bench.bench_inputs_with_multiple_files(input_file_name)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		elif bench_items == 'output':
			if bench_pattern == 'MFMW':
				data = bytes(rank for i in range(0, output_per_rank << 20))
				for _ in range(0, repeat_times):
					max_time, min_time, avg_time = cirrus_lustre_bench.bench_outputs_with_multiple_files(output_file_name, output_per_rank, data=data)
					__print_metrics(max_time, min_time, avg_time)
			else:
				raise ValueError('Unknown pattern ' + bench_pattern)
		else:
			raise ValueError('Unknown item ' + bench_items)
	else:
		raise ValueError('Unknown target ' + bench_targets)
	
def __print_metrics(*items):
	rank, _, _ = common.get_mpi_env()
	if 0 == rank:
		print(str(items)[1:-1])


if __name__ == '__main__':
	bench()