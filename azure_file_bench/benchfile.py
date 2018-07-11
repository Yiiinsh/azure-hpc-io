#! /usr/bin/env python3
' Benchmarking Azure file performance for HPC purpose'

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from azure.storage.file import FileService
from common import common

# Configurations
config_file = 'config.ini'
config_azure = common.get_config(config_file, 'AZURE')
config_bench = common.get_config(config_file, 'BENCH')

# MPI envs
rank, size, _ = common.get_mpi_env()

# Azure
file_service = FileService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
file_service.get_share_acl(config_azure['source_share_name'])

def bench_file_get_with_single_file_single_share():
	'''
	Benchmarking File get with multiple access on a single file within a single share

	Return:
	 type[] : source stream
	'''
	# Metrics
	max_read, min_read, avg_read = common.init_bench_metrics()

	for _ in range(0, int(config_bench['repeat_time'])):
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		file_service.get_file_to_bytes(config_azure['source_share_name'], None, config_azure['source_file_name'])
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
		file_size = file_service.get_file_properties(config_azure['source_share_name'], None, config_azure['source_file_name']).properties.content_length >> 20
		file_size = round(file_size, 3)
		bandwidth = round(file_size / avg_read, 3)
		print('-------- Azure File --------')
		print('-------- Single File, Multiple Reader --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Inputs on {1} Processes --------'.format(file_size, size))
		print('Latency: Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print()
	
	data = file_service.get_file_to_bytes(config_azure['source_share_name'], None, config_azure['source_file_name'])
	return data

def bench_file_write_with_single_file_single_share():
	'''
	Benchmarking File write with multiple access within a single share
	Data from different rank is stored in different ranges

	The processes is:
	 1. Create the file with specified size
	 2. Each process update their range of File
	
	Writing size per rank is defined in the config.ini
	'''
	# Data prepare
	write_size_per_rank = int(config_bench['write_size_per_rank']) # in MiB
	chunk_limit = int(config_azure['file_chunk_limit']) # in MiB
	section_size = write_size_per_rank << 20 # MiB -> byte
	chunk_size = write_size_per_rank // chunk_limit
	if write_size_per_rank % chunk_limit:
		chunk_size += 1
	data = [None for i in range(0, chunk_size)]
	for idx, section in enumerate(data):
		if idx == chunk_size - 1 and not write_size_per_rank % chunk_limit == 0:
			data[idx] = bytes(rank for i in range(0, (write_size_per_rank % chunk_limit) << 20)) # if last section doesn't fill in all the chunks
		else:
			data[idx] = bytes(rank for i in range(0, chunk_limit << 20))

	# Metrics
	avg_create = 0 # Average file create time
	avg_write_per_rank = 0 # Average write time for each rank putting their own blocks
	avg_write_all_ranks = 0 # Average write time for all ranks to finish writing and reach the barrier

	for _ in range(0, int(config_bench['repeat_time'])):
		# File create & resize
		if 0 == rank:
			create_start = MPI.Wtime()
			file_service.create_file(config_azure['source_share_name'], None, config_azure['output_file_name'], section_size * size)
			create_end = MPI.Wtime()

			avg_create += create_end - create_start

		# Writing
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for idx, content in enumerate(data):
			start_range = rank * section_size + idx * (chunk_limit << 20)
			end_range = start_range + len(content) - 1
			file_service.update_range(config_azure['source_share_name'], None, config_azure['output_file_name'], content, start_range, end_range)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		write_all_end = MPI.Wtime()

		_, _, avg_write_time = common.collect_get_bench_metrics(end - start)
		avg_write_per_rank += avg_write_time
		avg_write_all_ranks += write_all_end - start

		# Check results
		if 0 == rank:
			if not _check_single_file_correctness():
				return

	# Print results
	repeat_time = int(config_bench['repeat_time'])
	avg_write_per_rank = round(avg_write_per_rank / repeat_time, 3)
	avg_write_all_ranks = round(avg_write_all_ranks / repeat_time, 3)
	avg_create = round(avg_create / repeat_time, 3)
	total_write_time = avg_create + avg_write_all_ranks

	if 0 == rank:
		file_size = write_size_per_rank * size # MiB
		bandwidth = round(file_size / total_write_time, 3)
		proportion_write_per_rank = avg_write_per_rank / total_write_time
		proportion_write_all_ranks = avg_write_all_ranks / total_write_time
		proportion_create = avg_create / total_write_time

		print('-------- Azure File --------')
		print('-------- Single File, Multiple Writers --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Outputs on {1} Processes, {2} MiB Each --------'.format(file_size, size, write_size_per_rank))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print('Propotion: Write per process: {:.2%}, Write on all processes finish: {:.2%}'.format(proportion_write_per_rank, proportion_write_all_ranks))
		print('Propotion: Create: {:.2%}'.format(proportion_create))
		print()

def bench_file_write_with_multiple_files_single_share():
	'''
	Benchmarking File write with multiple access on multiple files within a single share
	Data from different ranks are stored in different blobs

	'''
	# Data prepare
	write_size_per_rank = int(config_bench['write_size_per_rank']) << 20 # in bytes
	data = bytes( rank for i in range(0, write_size_per_rank) )
	file_name = config_azure['output_file_name'] + '{:0>5}'.format(rank)

	# Metrics
	avg_write_per_rank = 0 # Average write time for each rank putting their own files
	avg_write_all_ranks = 0 # Average write time for all ranks to finish writing and reach the barrier

	for _ in range(0, int(config_bench['repeat_time'])):
		# Writing to files
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		file_service.create_file_from_bytes(config_azure['source_share_name'], None, file_name, data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		all_write_end = MPI.Wtime()

		_, _, avg_write_time = common.collect_get_bench_metrics(end - start)
		avg_write_per_rank += avg_write_time
		avg_write_all_ranks += all_write_end - start
	
	# Print results
	repeat_time = int(config_bench['repeat_time'])
	avg_write_per_rank = round(avg_write_per_rank / repeat_time, 3)
	avg_write_all_ranks = round(avg_write_all_ranks / repeat_time, 3)

	if 0 == rank:
		file_size = (write_size_per_rank >> 20) * size # in MiB
		bandwidth = round(file_size / avg_write_all_ranks, 3)
		propotion_write_per_rank = avg_write_per_rank / avg_write_all_ranks

		print('-------- Azure File --------')
		print('-------- Multiple Files, Multiple Writers --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Outputs on {1} Processes, {2} MiB Each --------'.format(file_size, size, write_size_per_rank >> 20))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print('Propotion: Write per process: {:.2%}'.format(propotion_write_per_rank))
		print()

def _check_single_file_correctness():
	'''
	Check the correctness of the file after write

	'''
	# Check file size
	write_size_per_rank = int(config_bench['write_size_per_rank']) << 20 # in byte
	file_size = file_service.get_file_properties(config_azure['source_share_name'], None, config_azure['output_file_name']).properties.content_length
	if file_size != write_size_per_rank * size:
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		return False
	
	# Check data for each section
	for r in range(0, size):
		start_range = r * write_size_per_rank
		end_range = (r + 1) * write_size_per_rank - 1
		data = file_service.get_file_to_bytes(config_azure['source_share_name'], None, config_azure['output_file_name'], start_range=start_range, end_range=end_range).content
		if data.count(bytes([r])) != write_size_per_rank:
			print('!!!!!!!!! Data Error !!!!!!!!!')
			print('!!!!!!!!! Data Error !!!!!!!!!')
			print('!!!!!!!!! Data Error !!!!!!!!!')
			return False
	
	return True