#! /usr/bin/env python3
'''
Common tools for azure-hpc-io benchmarking
'''
import configparser
from mpi4py import MPI

def get_mpi_env():
	'''
	Get MPI environmental parameters.

	return:
	 [int]rank : rank of current process
	 [int]size : size of processes used in MPI_COMM_WORLD
	 [str]processor_name : current processor name
	'''
	return MPI.COMM_WORLD.Get_rank(), MPI.COMM_WORLD.Get_size(), MPI.Get_processor_name()

def get_config(config_file, config_section):
	'''
	Get configuration section from specified configuration file.

	return:
	 [section]config : specified configuration section
	'''
	config = configparser.ConfigParser()
	config.read(config_file)

	if not config.has_section(config_section):
		raise AttributeError('Connot find config section')
	
	return config[config_section]

if __name__ == '__main__':
	rank, size, proc_name = get_mpi_env()
	print('Rank {0} of {1}. Processor name {2}'.format(rank, size, proc_name))
