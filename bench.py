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
from tool.base_bench import BaseBench

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

	# Get tool
	if bench_targets == 'azure_blob':
		bench_tool = AzureBlobBench(account_name, account_key, [input_container_name])
	elif bench_targets == 'azure_file':
		bench_tool = AzureFileBench(account_name, account_key, [input_share_name])
	elif bench_targets == 'cirrus_lustre':
		bench_tool = CirrusLustreBench()
	else:
		bench_tool = BaseBench(None, None, [])
	
	if bench_items == 'input':
		if bench_pattern == 'SFMR':
			for _ in range(0, repeat_times):
				max_time, min_time, avg_time = bench_tool.bench_inputs_with_single_file_multiple_readers(input_container_name, None, input_file_name)
				__print_metrics(max_time, min_time, avg_time)
		elif bench_pattern == 'MFMR':
			for _ in range(0, repeat_times):
				max_time, min_time, avg_time = bench_tool.bench_inputs_with_multiple_files_multiple_readers(input_container_name, None, input_file_name)
				__print_metrics(max_time, min_time, avg_time)
		else:
			raise NotImplementedError()
	elif bench_items == 'output':
		if bench_pattern == 'SFMW':
			data = common.workload_generator(rank, output_per_rank << 20)
			for _ in range(0, repeat_times):
				max_time, min_time, avg_time, post_time = bench_tool.bench_outputs_with_single_file_multiple_writers(output_container_name, None, output_file_name, output_per_rank, data)
				__print_metrics(max_time, min_time, avg_time, post_time)
		elif bench_pattern == 'MFMW':
			data = common.workload_generator(rank, output_per_rank << 20)
			for _ in range(0, repeat_times):
				max_time, min_time, avg_time = bench_tool.bench_outputs_with_multiple_files_multiple_writers(output_container_name, None, output_file_name, output_per_rank, data = data)
				__print_metrics(max_time, min_time, avg_time)
		else:
			raise NotImplementedError()

def __print_metrics(*items):
	rank, _, _ = common.get_mpi_env()
	if 0 == rank:
		print(str(items)[1:-1])


if __name__ == '__main__':
	bench()