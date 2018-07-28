from mpi4py import MPI
from azure.storage import blob
from tool.base_bench import BaseBench
from common import common

class AzureBlobBench(BaseBench):
	'''
	Tools for benchmarking Azure Blob\'s performance for HPC purpose.
	MPI is used for process management.

	param:
	 access_name: Storage target access name
	 access_key: Storage target access key
	 access_container_list: Containers to be accessed
	'''
	# Azure Blob limits
	BLOCK_LIMIT = 100 # in MiB
	SECTION_LIMIT = 1024 # in MiB
	BLOCK_LIMIT_IN_BYTES = BLOCK_LIMIT << 20 # in bytes
	SECTION_LIMIT_IN_BYTES = SECTION_LIMIT << 20 # in bytes

	__slots__ = ('__bench_target', '__mpi_rank', '__mpi_size', '__storage_service')

	def __init__(self, access_name, access_key, access_container_list):
		super.__init__(access_name, access_key, access_container_list)

		self.__bench_target = 'Azure Blob'
		self.__storage_service = blob.BlockBlobService(account_name=access_name, account_key=access_key)

		if not isinstance(access_container_list, list):
			raise TypeError('access_container_list should be a list!')
		for container in access_container_list:
			self.__storage_service.get_container_acl(container)

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
		# Sections to be get
		blob_size = self.__storage_service.get_blob_properties(container_name, file_name).properties.content_length  # in bytes
		blob_size_in_mib = blob_size >> 20  # in MiB
		# Get operations to be performed
		section_count = blob_size_in_mib // self.SECTION_LIMIT
		if blob_size_in_mib % self.SECTION_LIMIT:
			section_count = section_count + 1

		MPI.COMMWOR.Barrier()
		start = MPI.Wtime()
		for section in range(0, section_count):
			range_start = section * self.SECTION_LIMIT_IN_BYTES
			range_end = range_start + self.SECTION_LIMIT_IN_BYTES - 1
			if range_start > blob_size - 1:
				break
			if range_end > blob_size - 1:
				range_end = blob_size - 1
			self.__storage_service.get_blob_to_bytes(container_name, file_name, start_range=range_start, end_range=range_end)
			
		end = MPI.Wtime()
		MPI.COMMWOR.Barrier()

		return common.collect_bench_metrics(end - start)

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
		proc_blob_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		return self.bench_inputs_with_single_file_multiple_readers(container_name, directory_name, proc_blob_name)

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
		proc_blob_name = file_name + '{:0>5}'.format(self.__mpi_rank)
		
		return self.bench_inputs_with_single_file_multiple_readers(proc_container_name, directory_name, proc_blob_name)

	def bench_outputs_with_single_file_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Single File Multiple Writers`
		
		Each processes will access a single shared file in different sections exclusively.

		Data from different rank is stored in different blocks

		Pattern of global block ids: 00002-00005, first section represents for the rank while the second section represents block id written by the rank

		The process is:
		1. Each rank write blocks to Azure
		2. MPI_Barrier() to wait for all ranks
		3. Get uncommited block list, rearrange for the order of data
		4. Commit changes

		param:
		 container_name: target container
		 directory_name: target directory
		 file_name: target file
		 output_per_rank: size of outputs per rank in MiB
		 data: optional cached data for outputs, in this case stands for data of a full block(100 MiB data)
		
		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		'''
		# Data prepare
		if data == None:
			data = common.workload_generator(self.__mpi_rank, self.BLOCK_LIMIT_IN_BYTES)
		last_block_data = data
		block_count = output_per_rank // self.BLOCK_LIMIT
		# Last block doesn't full
		if output_per_rank % self.BLOCK_LIMIT:
			block_count = block_count + 1
			last_block_data = common.workload_generator(self.__mpi_rank, (output_per_rank % self.BLOCK_LIMIT) << 20)
		
		# Step.1 put blocks
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for i in range(0, block_count):
			block_id = '{:0>5}-{:0>5}'.format(self.__mpi_rank, i)
			if i != (block_count - 1):
				self.__storage_service.put_block(container_name, file_name, data, block_id)
			elif i == (block_count - 1):
				self.__storage_service.put_block(container_name, file_name, last_block_data, block_id)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		max_write, min_write, avg_write = common.collect_bench_metrics(end - start)

		if 0 == self.__mpi_rank:
			start_postprocessing = MPI.Wtime()
			# Step.3 get block list and sort according to block id
			block_list = self.__storage_service.get_block_list(container_name, file_name, block_list_type=blob.BlockListType.All).uncommitted_blocks
			block_list.sort(key = lambda block: block.id)

			# Step.4 commit
			self.__storage_service.put_block_list(container_name, file_name, block_list)
			end_postprocessing = MPI.Wtime()

			postprocessing_time = end_postprocessing - start_postprocessing
			max_write = max_write + postprocessing_time
			min_write = min_write + postprocessing_time
			avg_write = avg_write + postprocessing_time
		
		return max_write, min_write, avg_write

	def bench_outputs_with_multiple_files_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Multiple Files Multiple Writers`
		
		Each processes will access a single file in different containers exclusively.

		Pattern of output blobs is: blob_name + 00001 where the second parts represents for the rank of the process 

		param:
		 container_name: target container base, target container name is composed of container_name + '{:0>5}'.format(__mpi_rank)
		 directory_name: target container directory
		 file_name: target file base, target file name is composed of file_name + '{:0>5}'.format(__mpi_rank)
		 output_per_rank: size of outputs per rank in MiB
		 data: optional cached data for outputs, currently only data less than SECTION_LIMIT is allowed

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
		
		output_blob_name = file_name + '{:0>5}'.format(self.__mpi_rank)

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		self.__storage_service.create_blob_from_bytes(container_name, output_blob_name, data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_outputs_with_multiple_files_multiple_writers_multiple_containers(self, container_name, directory_name, file_name, output_per_rank, data = None):
		'''
		Benchmarking outputs with pattern `Multiple Files Multiple Writers`
		
		Each processes will access a single file in different containers exclusively.

		Pattern of output blobs is: blob_name + 00001 where the second parts represents for the rank of the process 

		Pattern of output container is: container_name + 00001 where the second parts represents for the rank of the process

		param:
		 container_name: target container base, target container name is composed of container_name + '{:0>5}'.format(__mpi_rank)
		 directory_name: target container directory
		 file_name: target file base, target file name is composed of file_name + '{:0>5}'.format(__mpi_rank)
		 output_per_rank: size of outputs per rank in MiB
		 data: optional cached data for outputs, currently only data less than SECTION_LIMIT is allowed

		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		'''
		output_container_name = container_name + '{:0>5}'.format(self.__mpi_rank)

		return self.bench_outputs_with_multiple_files_multiple_writers(output_container_name, directory_name, file_name, output_per_rank, data)