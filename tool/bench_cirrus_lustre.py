import os
from mpi4py import MPI
from tool.base_bench import BaseBench
from common import common

class CirrusLustreBench(BaseBench):
	''' 
	Tools for benchmarking Cirrus Lustre\'s performance for HPC purpose.
	MPI is used for process management.

	'''
	# File Limits
	SECTION_LMIT = 1024 # in MiB
	SECTION_LIMIT_IN_BYTES = SECTION_LMIT << 20 # in bytes

	__slots__=('__mpi_rank', '__mpi_size')

	def __init__(self):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()

	def bench_inputs_with_single_file_multiple_readers(self, container_name, directory_name, file_name):
		'''
		Benchmarking inputs with pattern `Single File Multiple Readers`

		param:
		 container_name: source container
		 directory_name: source directory
		 file_name: source file

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		# Sections to be get
		file_size = os.path.getsize(file_name)
		file_size_in_mib = file_size >> 20 # in MiB
		section_count = file_size_in_mib // self.SECTION_LMIT
		if file_size_in_mib % self.SECTION_LMIT:
			section_count = section_count + 1
		
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		if section_count == 1:
			with open(file_name, 'r') as f:
				f.read()
		else:
			with open(file_name, 'r') as f:
				for _ in range(0, section_count):
					f.read(self.SECTION_LIMIT_IN_BYTES)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start, 5)

	def bench_inputs_with_multiple_files_multiple_readers(self, container_name, directory_name, file_name):
		'''
		Benchmarking inputs with pattern `Multiple Files Multiple Readers`
		
		Each processes will access a single file within the same container exclusively.

		param:
		 container_name: source container
		 directory_name: source directory
		 file_name: source file base, source file name for each processes is composed of file_name + '{:0>5}'.format(__mpi_rank)

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		proc_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		return self.bench_inputs_with_single_file_multiple_readers(container_name, directory_name, proc_file_name)
	

	def bench_outputs_with_multiple_files_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Multiple Files Multiple Writers`
		
		Each processes will access a single file within the same container exclusively.

		param:
		 container_name: target container base
		 directory_name: target directory
		 file_name: target file base, target file name is composed of file_name + '{:0>5}'.format(__mpi_rank)
		 output_per_rank: size of outputs per rank in MiB
		 data: optional cached data for outputs
		
		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		'''
		# Data prepare
		output_per_rank_in_bytes = output_per_rank << 20 # in bytes
		if data == None:
			data = common.workload_generator(self.__mpi_rank, output_per_rank_in_bytes)

		output_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		with open(output_file_name, 'wb') as f:
			f.write(data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start, 5)