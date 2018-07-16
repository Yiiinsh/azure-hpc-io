import os
from mpi4py import MPI
from common import common

class CirrusLustreBench(object):
	''' 
	Tools for benchmarking Cirrus Lustre\'s performance for HPC purpose.
	MPI is used for process management.

	'''
	__slots__=('__mpi_rank', '__mpi_size')

	def __init__(self):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()
	
	def __str__(self):
		return '[CirrusLustreBench]: on rank {0} out of {1}'.format(self.__mpi_rank, self.__mpi_size)
	
	__repr__ = __str__

	def bench_inputs_with_single_file(self, file_name):
		'''
		Benchmarking lustre file get with multiple access on a single file
		
		If the size of source file is larger than section limits, the get operation will be divided into serval sections, with getting size of section_limit each time.
		
		For benchmarking on a single large section, the input shoud be set to a proper size that can be divided by MPI_SIZE * section_limit

		param:
		 file_name: File name
		 section_limit: Limit of sections for each get operation in MiB
		
		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		with open(file_name, 'r') as f:
			f.read()
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)
	
	def bench_inputs_with_multiple_files(self, file_name):
		'''
		Benchmarking lustre file get with multiple access on multiple files
		
		Files corresponding to each processes should be named after the pattern of file_name + rank

		param:
		 file_name: File name

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		proc_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		return self.bench_inputs_with_single_file(proc_file_name)

	def bench_outputs_with_single_file(self, file_name, output_per_rank = 1024):
		'''
		Benchmarking lustre file write with multiple access on a single file
		
		Data from different rank is stored in different ranges

		param:
		 file_name: File name
		 output_per_rank: size of outputs per rank, in MiB
		'''
		raise NameError('Unsupport pattern')

	def bench_outputs_with_multiple_files(self, file_name, output_per_rank = 1024):
		'''
		Benchmarking lustre file write with multiple access
		
		Data from different rank is stored in different files

		param:
		 file_name: File name
		 output_per_rank: size of outputs per rank, in MiB
		
		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		'''
		# Data prepare
		output_per_rank_in_bytes = output_per_rank << 20
		data = bytes( self.__mpi_rank for i in range(0, output_per_rank_in_bytes) )
		output_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		with open(output_file_name, 'wb') as f:
			f.write(data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)