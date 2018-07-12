#! /usr/bin/env python3
' Benchmarking Azure blob performance for HPC purpose'

import os, sys, configparser
import numpy as np
from mpi4py import MPI
from azure.storage.blob import BlockBlobService, BlobBlock, BlockListType
from common import common

# Configurations
config_file = 'config.ini'
config_azure = common.get_config(config_file, 'AZURE')
config_bench = common.get_config(config_file, 'BENCH')

# MPI envs
rank, size, _ = common.get_mpi_env()

def bench_blob_get_with_single_blob_single_container():
	'''
	Benchmarking Blob get with multiple access on a single blob within a single container

	'''
	# Azure
	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	
	# Metrics
	max_read, min_read, avg_read = common.init_bench_metrics()

	for _ in range(0, int(config_bench['repeat_time'])):
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		block_blob_service.get_blob_to_bytes(config_azure['source_container_name'], config_azure['source_blob_name'])
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
		file_size = block_blob_service.get_blob_properties(config_azure['source_container_name'], config_azure['source_blob_name']).properties.content_length >> 20
		file_size = round(file_size, 3)
		bandwidth = round(file_size / avg_read, 3)
		print('-------- Azure Blob --------')
		print('-------- Single Blob, Multiple Reader --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Inputs on {1} Processes --------'.format(file_size, size))
		print('Latency: Max {0} s, Min {1} s, Avg {2} s'.format(max_read, min_read, avg_read))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print()

def bench_block_blob_write_with_single_blob_single_container():
	'''
	Benchmarking Block Blob write with multiple access on a single blob within a single container
	Data from different rank is stored in different blocks

	The processes is :
	 1. Each rank write blocks to Azure 
	 2. MPI_Barrier() to wait for all ranks
	 3. Get uncommited block list, rearrange for order
	 4. Commit

	Writing size per rank is defined in the config.ini
	Format of global block ids: 00002-00005, first section represents for the rank and second section represents block id written by the rank

	'''
	# Azure
	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	
	# Data prepare
	write_size_per_rank = int(config_bench['write_size_per_rank']) # in MiB
	blob_block_limit = int(config_azure['blob_block_limit']) # in MiB
	section_size = write_size_per_rank << 20 # MiB -> Byte
	block_size = write_size_per_rank // blob_block_limit
	if write_size_per_rank % blob_block_limit:
		block_size += 1
	data = [None for i in range(0, block_size)]
	for idx, _ in enumerate(data):
		if idx == block_size - 1 and write_size_per_rank % blob_block_limit:
			data[idx] = bytes( rank for i in range(0, (write_size_per_rank % blob_block_limit) << 20 ) ) # if last section doesn't fill in all the block
		else:
			data[idx] = bytes( rank for i in range(0, blob_block_limit << 20) )
	
	# Metrics
	avg_write_per_rank = 0 # Average write time for each rank putting their own blocks
	avg_write_all_ranks = 0 # Average write time for all ranks to finish writing and reach the barrier
	avg_pre_commit = 0 # Average time for preprocessing: get uncommited block list, rearrange the order
	avg_commit = 0 # Average time for commit blocks

	for _ in range(0, int(config_bench['repeat_time'])):
		# Writing to Blocks
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for idx, content in enumerate(data):
			block_id = '{:0>5}-{:0>5}'.format(rank, idx)
			block_blob_service.put_block(config_azure['source_container_name'], config_azure['output_blob_name'], content, block_id)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		write_all_end = MPI.Wtime()

		_, _, avg_write_time = common.collect_get_bench_metrics(end - start)
		avg_write_per_rank += avg_write_time
		avg_write_all_ranks += write_all_end - start

		# Master handle
		if 0 == rank:
			# Get block list to be committed and rearrange
			pre_commit_start = MPI.Wtime()
			block_list = block_blob_service.get_block_list(config_azure['source_container_name'], config_azure['output_blob_name'], block_list_type=BlockListType.All).uncommitted_blocks
			block_list.sort(key = lambda block:block.id)
			pre_commit_end = MPI.Wtime()
			avg_pre_commit += pre_commit_end - pre_commit_start

			# Commit
			commit_start = MPI.Wtime()
			block_blob_service.put_block_list(config_azure['source_container_name'], config_azure['output_blob_name'], block_list)
			commit_end = MPI.Wtime()
			avg_commit += commit_end - commit_start

			# Check correctness
			if not _check_single_blob_correctness():
				# Error !!! Return
				return

	# Print result
	repeat_time = int(config_bench['repeat_time'])
	avg_write_per_rank = round(avg_write_per_rank / repeat_time, 3)
	avg_write_all_ranks = round(avg_write_all_ranks / repeat_time, 3)
	avg_pre_commit = round(avg_pre_commit / repeat_time, 3)
	avg_commit = round(avg_commit / repeat_time, 3)
	total_write_time = avg_write_all_ranks + avg_pre_commit + avg_commit

	if 0 == rank:
		file_size = write_size_per_rank * size # MiB
		bandwidth = round(file_size / (avg_write_all_ranks + avg_pre_commit + avg_commit), 3)
		propotion_write_per_rank = avg_write_per_rank / total_write_time
		propotion_write_all_ranks = avg_write_all_ranks / total_write_time
		propotion_pre_commit = avg_pre_commit / total_write_time
		propotion_commit = avg_commit / total_write_time

		print('-------- Azure Blob --------')
		print('-------- Single Blob, Multiple Writers --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Outputs on {1} Processes, {2} MiB Each --------'.format(file_size, size, write_size_per_rank))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print('Propotion: Write per process: {:.2%}, Write on all processes finish: {:.2%}'.format(propotion_write_per_rank, propotion_write_all_ranks))
		print('Propotion: Pre commit processing: {:.2%}, Commit: {:.2%}'.format(propotion_pre_commit, propotion_commit))
		print()

def bench_block_blob_write_with_multiple_blob_single_container():
	'''
	Benchmarking Block Blob write with multiple access on multiple blobs within a single container
	Data from different ranks are stored in different blobs

	'''
	# Azure
	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	
	# Data prepare
	write_size_per_rank = int(config_bench['write_size_per_rank']) << 20 # in Bytes
	data = bytes( rank for i in range(0, write_size_per_rank))
	blob_name = config_azure['output_blob_name'] + '{:0>5}'.format(rank)
	
	# Metrics
	avg_write_per_rank = 0 # Average write time for each rank putting their own blobs
	avg_write_all_ranks = 0 # Average write time for all ranks to finish writing and reach the barrier

	for _ in range(0, int(config_bench['repeat_time'])):
		# Writing to blobs
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		block_blob_service.create_blob_from_bytes(config_azure['source_container_name'], blob_name, data)
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

		print('-------- Azure Blob --------')
		print('-------- Multiple Blobs, Multiple Writers --------')
		print('-------- Repeat {0} times --------'.format(config_bench['repeat_time']))
		print('-------- {0} MiB Outputs on {1} Processes, {2} MiB Each --------'.format(file_size, size, write_size_per_rank >> 20))
		print('Bandwidth: {0} MiB/s'.format(bandwidth))
		print('Propotion: Write per process: {:.2%}'.format(propotion_write_per_rank))
		print()

def _check_single_blob_correctness():
	'''
	Check the correctness of the blob after write

	'''
	# Azure
	block_blob_service = BlockBlobService(account_name=config_azure['account_name'], account_key=config_azure['account_key'])
	block_blob_service.get_container_acl(config_azure['source_container_name'])
	
	# Check file size
	write_size_per_rank = int(config_bench['write_size_per_rank']) << 20 # in Byte
	file_size = block_blob_service.get_blob_properties(config_azure['source_container_name'], config_azure['output_blob_name']).properties.content_length
	if file_size != write_size_per_rank * size:
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		print('!!!!!!!!! Size Not Match !!!!!!!!!')
		return False
	
	# Check data for each section
	for r in range(0, size):
		start_range = r * write_size_per_rank
		end_range = (r + 1) * write_size_per_rank - 1
		data = block_blob_service.get_blob_to_bytes(config_azure['source_container_name'], config_azure['output_blob_name'], start_range=start_range, end_range=end_range).content
		if data.count(bytes([r])) != write_size_per_rank:
			print('!!!!!!!!! Data Error !!!!!!!!!')
			print('!!!!!!!!! Data Error !!!!!!!!!')
			print('!!!!!!!!! Data Error !!!!!!!!!')
			return False

	return True