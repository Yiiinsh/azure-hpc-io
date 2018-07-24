from mpi4py import MPI
from azure.storage.file import FileService
from common import common

class AzureFileBench(object):
	''' 
	Tools for benchmarking Azure File\'s performance for HPC purpose.
	MPI is used for process management.

	param:
	 account_name: Azure Storage account name
	 account_key: Azure Storage account key
	 access_container_list: Containers to be accessed
	'''
	__slots__=('__mpi_rank', '__mpi_size', '__file_service')

	def __init__(self, account_name, account_key, access_share_list):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()

		self.__file_service = FileService(account_name, account_key)

		if not isinstance(access_share_list, list):
			raise TypeError('access_share_list should be a list!')
		for share in access_share_list:
			self.__file_service.get_share_acl(share)
	
	def __str__(self):
		return '[AzureFileBench]: on rank {0} out of {1}'.format(self.__mpi_rank, self.__mpi_size)
	
	__repr__ = __str__

	def bench_inputs_with_single_file(self, share_name, directory_name, file_name, section_limit = 1024):
		'''
		Benchmarking File get with multiple access on a single file within a single share, single directory.
		
		If the size of source blob is larger than limits, the get operation will be divided into serval sections, with getting size of section_limit each time.
		
		For benchmarking on a single large section, the input shoud be set to a proper size that can be divided by MPI_SIZE * section_limit

		param:
		 share_name: File share
		 file_name: File name
		 directory_name: Directory name
		 section_limit: Limit of sections for each get operation in MiB
		
		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		# Check sections to be get
		file_size = self.__file_service.get_file_properties(share_name, directory_name, file_name).properties.content_length
		file_size_in_mib = file_size >> 20 # in MiB
		section_limit_in_bytes = section_limit << 20 # in bytes
		section_count = 0
		if file_size_in_mib <= section_limit:
			section_count = 1
		else:
			if file_size_in_mib % self.__mpi_size:
				raise ValueError('file size cannot be divided by mpi size')
			section_count = file_size_in_mib // self.__mpi_size
			if section_count % section_limit:
				section_count = section_count // section_limit + 1
			else:
				section_count = section_count // section_limit

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		if section_count == 1:
			self.__file_service.get_file_to_bytes(share_name, directory_name, file_name)
		else:
			for section in range(0, section_count):
				range_start = section * (self.__mpi_size * section_limit_in_bytes) + self.__mpi_rank * section_limit_in_bytes
				range_end = range_start + section_limit_in_bytes - 1
				if range_start > file_size - 1:
					break
				if range_end > file_size - 1:
					range_end = file_size - 1
				self.__file_service.get_file_to_bytes(share_name, directory_name, file_name, start_range=range_start, end_range=range_end)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_inputs_with_multiple_files(self, share_name, directory_name, file_name, section_limit = 1024):
		'''
		Benchmarking File get with multiple access on multiple processes within a single share.
		File size should be valid.
		Files corresponding to each processes should be named after the pattern of file_name + rank

		param:
		 share_name: File share
		 directory_name: Directory name
		 file_name: File name
		 section_limit: Limit of sections for each get operation in MiB

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		proc_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		# Check sections to be get
		file_size = self.__file_service.get_file_properties(share_name, directory_name, proc_file_name).properties.content_length
		file_size_in_mib = file_size >> 20 # in MiB
		section_limit_in_bytes = section_limit << 20 # in bytes
		section_count = file_size_in_mib // section_limit
		if file_size_in_mib % section_limit:
			section_count += 1
		
		# Get
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for section in range(0, section_count):
			range_start = section * section_limit_in_bytes
			range_end = range_start + section_limit_in_bytes - 1
			if range_start > file_size - 1:
				break
			if range_end > file_size - 1:
				range_end = file_size - 1
			self.__file_service.get_file_to_bytes(share_name, directory_name, proc_file_name, start_range=range_start, end_range=range_end)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_outputs_with_single_file(self, share_name, directory_name, file_name, output_per_rank = 1024, file_chunk_limit = 4):
		'''
		Benchmarking File write with multiple access within a single share
		Data from different rank is stored in different ranges

		The processes is:
		 1. Create the file with specified size
		 2. Each process update their range of File
		
		param:
		 share_name: File share name
		 directory_name: File directory name
		 file_name: File name
		 output_per_rank: size of outputs per rank, in MiB
		 file_chunk_limit: limits of write each time, in MiB
		
		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		 preprocessing_time: pre processing time, stands for step 1
		'''
		# Data prepare
		output_per_rank_in_bytes = output_per_rank << 20 # in bytes
		file_chunk_limit_in_bytes = file_chunk_limit << 20 # in bytes
		data = bytes(self.__mpi_rank for i in range(0, file_chunk_limit_in_bytes))
		data_last_chunk = data
		chunk_size = output_per_rank // file_chunk_limit
		if output_per_rank % file_chunk_limit:
			chunk_size += 1
			last_chunk_size = (output_per_rank % file_chunk_limit) << 20 # in bytes
			data_last_chunk = bytes(self.__mpi_rank for i in range(0, last_chunk_size))

		# File create
		create_start = 0
		create_end = 0
		if 0 == self.__mpi_rank:
			create_start = MPI.Wtime()
			self.__file_service.create_file(share_name, directory_name, file_name, output_per_rank_in_bytes * self.__mpi_size)
			create_end = MPI.Wtime()
		preprocessing_time = create_end - create_start
		
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for i in range(0, chunk_size):
			if i != (chunk_size - 1):
				start_range = self.__mpi_rank * output_per_rank_in_bytes + i * file_chunk_limit_in_bytes
				end_range = start_range + len(data) - 1
				self.__file_service.update_range(share_name, directory_name, file_name, data, start_range, end_range)
			elif i == (chunk_size - 1):
				start_range = self.__mpi_rank * output_per_rank_in_bytes + i * file_chunk_limit_in_bytes
				end_range = start_range + len(data_last_chunk) - 1
				self.__file_service.update_range(share_name, directory_name, file_name, data_last_chunk, start_range, end_range)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		max_write, min_write, avg_write = common.collect_bench_metrics(end - start)

		# TODO: Every process check their own part

		return max_write, min_write, avg_write, preprocessing_time

	def bench_outputs_with_multiple_files(self, share_name, directory_name, file_name, output_per_rank = 1024, file_chunk_limit = 4, data = None):
		'''
		Benchmarking File write with multiple access within a single share
		Data from different rank is stored in different files

		The processes is:
		 1. Create the file with specified size
		 2. Each process update their File
		
		param:
		 share_name: File share name
		 directory_name: File directory name
		 file_name: File name
		 output_per_rank: size of outputs per rank, in MiB
		 file_chunk_limit: limits of write each time, in MiB
		
		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		 max_preprocessing_time: maximum pre processing time, stands for step 1
		 min_preprocessing_time: minimum pre processing time, stands for step 1
		 avg_preprocessing_time: average pre processing time, stands for step 1
		'''
		# Data prepare
		if output_per_rank > 1025:
			raise ValueError('Not support for large file size currently')
		output_per_rank_in_bytes = output_per_rank << 20 # in bytes
		if data == None:
			data = bytes(self.__mpi_rank for i in range(0, output_per_rank_in_bytes))
		output_file_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		# Output
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		self.__file_service.create_file_from_bytes(share_name, directory_name, output_file_name, data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		max_time, min_time, avg_time = common.collect_bench_metrics(end - start)

		return max_time, min_time, avg_time