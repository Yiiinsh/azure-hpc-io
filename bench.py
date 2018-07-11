#! /usr/bin/env python3
'''
Benchmarking I/O performance for HPC purpose
'''

from mpi4py import MPI
from common import common
from cirrus_lustre_bench import benchcirrus
from azure_blob_bench import benchblob
from azure_file_bench import benchfile

def bench():
	# Configurations
	config_file = 'config.ini'
	config_bench = common.get_config(config_file, 'BENCH')

	# MPI envs
	if bool(config_bench['show_mpi_env']):
		rank, size, proc_name = common.get_mpi_env()
		print('Rank {0} of {1}. Proc name:{2}'.format(rank, size, proc_name))
		print()

	# Benchmarking
	bench_items = config_bench['bench_items'].split(',')
	bench_targets = config_bench['bench_targets'].split(',')
	for item in bench_items:
		if item == 'input':
			for target in bench_targets:
				if target == 'cirrus':
					benchcirrus.bench_cirrus_get_with_posix_io()
				elif target == 'azure_blob':
					benchblob.bench_blob_get_with_single_blob_single_container()
				elif target == 'azure_file':
					benchfile.bench_file_get_with_single_file_single_share()
		elif item == 'output':
			for target in bench_targets:
				if target == 'azure_blob':
					benchblob.bench_block_blob_write_with_single_blob_single_container()

					benchblob.bench_block_blob_write_with_multiple_blob_single_container()
				elif target == 'azure_file':
					benchfile.bench_file_write_with_single_file_single_share()

					benchfile.bench_file_write_with_multiple_files_single_share()
	
if __name__ == '__main__':
	bench()