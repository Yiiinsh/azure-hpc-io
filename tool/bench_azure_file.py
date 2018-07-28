from mpi4py import MPI
from azure.storage import file
from tool.base_bench import BaseBench
from common import common

class AzureFileBench(BaseBench):
	''' 
	Tools for benchmarking Azure File\'s performance for HPC purpose.
	MPI is used for process management.

	param:
	 access_name: Storage target access name
	 access_key: Storage target access key
	 access_container_list: Containers to be accessed
	'''
	# Azure File Limits
	SECTION_LIMIT = 1024 # in MiB
	FILE_CHUNK_LIMIT = 4 # in MIB
	SECTION_LIMIT_IN_BYTES = SECTION_LIMIT << 20 # in bytes
	FILE_CHUNK_LIMIT_IN_BYTES = FILE_CHUNK_LIMIT << 20 # in bytes

	__slots__ = ('__bench_target', '__mpi_rank', '__mpi_size', '__storage_service')

	def __init__(self, access_name, access_key, access_container_list):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()
		self.__bench_target = 'Azure File'
		self.__storage_service = file.FileService(access_name, access_key)

	def bench_inputs_with_single_file_multiple_readers(self, container_name, directory_name, file_name):
		'''
		Benchmarking inputs with pattern `Single File Multiple Readers`

		For benchmarking on large sources, the entier data will be divided into serveral sections with size of SECTION_LIMIT, 
		the read operations will be performed sequentially on each sections. Every processes will read the entire data individually.

		param:
		 container_name: source container
		 directory_name: source directory
		 file_name: source file

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		# sections to be get
		file_size = self.__storage_service.get_file_properties(container_name, directory_name, file_name).properties.content_length
		file_size_in_mib = file_size >> 20 # in MiB
		section_count = file_size_in_mib // self.SECTION_LIMIT
		if file_size_in_mib % self.SECTION_LIMIT:
			section_count = section_count + 1
		
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for section in range(0, section_count):
			range_start = section * self.SECTION_LIMIT_IN_BYTES
			range_end = range_start + self.SECTION_LIMIT_IN_BYTES - 1
			if range_start > file_size - 1:
				break
			if range_end > file_size - 1:
				range_end = file_size - 1
			self.__storage_service.get_file_to_bytes(container_name, directory_name, file_name, start_range=range_start, end_range=range_end)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_inputs_with_multiple_files_multiple_readers(self, container_name, directory_name, file_name):
		'''
		Benchmarking inputs with pattern `Multiple Files Multiple Readers`
		
		Each processes will access a single file within the same container exclusively.

		Files corresponding to each processes are named after the pattern of file_name + rank

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

	def bench_inputs_with_multiple_files_multiple_readers_multiple_containers(self, container_name, directory_name, file_name):
		'''
		Benchmarking inputs with pattern `Multiple Files Multiple Readers`
		
		Each processes will access a single file in different containers exclusively.

		param:
		 container_name: source container base, source container name for each processes is composed of container_name + '{0>5}'.format(__mpi_rank)
		 directory_name: source directory
		 file_name: source file base, source file name for each processes is composed of file_name + '{:0>5}'.format(__mpi_rank)

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		proc_container_name = container_name + '{:0>5}'.format(self.__mpi_rank)
		proc_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)
		
		return self.bench_inputs_with_single_file_multiple_readers(proc_container_name, directory_name, proc_file_name)

	def bench_outputs_with_single_file_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Single File Multiple Writers`
		
		Each processes will access a single shared file in different sections exclusively.

		Data fro mdifferent rank is stored in different ranges

		The processes is:
		 1. Create the file with specified size
		 2. Each process update their range of File

		param:
		 container_name: target container
		 directory_name: target directory
		 file_name: target file
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
			data = common.workload_generator(self.__mpi_rank, self.FILE_CHUNK_LIMIT_IN_BYTES)
		data_last_chunk = data
		chunk_count = output_per_rank // self.FILE_CHUNK_LIMIT
		# Last chunk doesn't full
		if output_per_rank % self.FILE_CHUNK_LIMIT:
			chunk_count = chunk_count + 1
			data_last_chunk = common.workload_generator(self.__mpi_rank, (output_per_rank % self.FILE_CHUNK_LIMIT) << 20)

		# Step .1 File create
		create_start = 0
		create_end = 0
		if 0 == self.__mpi_rank:
			create_start = MPI.Wtime()
			self.__storage_service.create_file(container_name, directory_name, file_name, output_per_rank_in_bytes * self.__mpi_size)
			create_end = MPI.Wtime()
		create_time = create_end - create_start

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for i in range(0, chunk_count):
			if i != (chunk_count - 1):
				start_range = self.__mpi_rank * output_per_rank_in_bytes + i * self.FILE_CHUNK_LIMIT_IN_BYTES
				end_range = start_range + len(data) - 1
				self.__storage_service.update_range(container_name, directory_name, file_name, data, start_range, end_range)
			elif i == (chunk_count - 1):
				start_range = self.__mpi_rank * output_per_rank_in_bytes + i * self.FILE_CHUNK_LIMIT_IN_BYTES
				end_range = start_range + len(data_last_chunk) - 1
				self.__storage_service.update_range(container_name, directory_name, file_name, data_last_chunk, start_range, end_range)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		max_write, min_write, avg_write = common.collect_bench_metrics(end - start)
		max_write = max_write + create_time
		min_write = min_write + create_time
		avg_write = avg_write + create_time

		return max_write, min_write, avg_write

	def bench_outputs_with_multiple_files_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Multiple Files Multiple Writers`
		
		Each processes will access a single file within the same container exclusively.

		Pattern of output blobs is: file_name + 00001 where the second parts represents for the rank of the process 

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
		if output_per_rank > self.SECTION_LIMIT:
			raise ValueError('Not support for {} MiB output per rank now'.format(output_per_rank))
		if data == None:
			output_per_rank_in_bytes = output_per_rank << 20
			data = common.workload_generator(self.__mpi_rank, output_per_rank_in_bytes)
		
		output_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		self.__storage_service.create_file_from_bytes(container_name, directory_name, output_file_name, data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_outputs_with_multiple_files_multiple_writers_multiple_containers(self, container_name, directory_name, file_name, output_per_rank, data):
		'''
		Benchmarking outputs with pattern `Multiple Files Multiple Writers`
		
		Each processes will access a single file in different containers exclusively.

		param:
		 container_name: target container base, target container name is composed of container_name + '{:0>5}'.format(__mpi_rank)
		 directory_name: target container directory
		 file_name: target file base, target file name is composed of file_name + '{:0>5}'.format(__mpi_rank)
		 output_per_rank: size of outputs per rank in MiB
		 data: optional cached data for outputs

		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		'''
		output_container_name = container_name + '{:0>5}'.format(self.__mpi_rank)

		return self.bench_outputs_with_multiple_files_multiple_writers(output_container_name, directory_name, file_name, output_container_name, data)