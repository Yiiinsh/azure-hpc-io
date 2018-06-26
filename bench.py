#! /usr/bin/env python3
'''
Benchmarking I/O performance for HPC purpose
'''

from mpi4py import MPI
from common import common
from cirrus_lustre_bench import benchcirrus
from azure_blob_bench import benchblob

def bench():
	# Configurations
	config_file = 'config.ini'
	config_bench = common.get_config(config_file, 'BENCH')

	# MPI envs
	rank, size, proc_name = common.get_mpi_env()
	print('Rank {0} of {1}. Proc name:{2}'.format(rank, size, proc_name))

	bench_targets = config_bench['bench_targets'].split(',')
	for target in bench_targets:
		if target == 'cirrus':
			benchcirrus.bench_cirrus_get_with_posix_io()
		elif target == 'azure_blob':
			benchblob.bench_blob_get_with_single_blob_single_container()
	

if __name__ == '__main__':
	bench()