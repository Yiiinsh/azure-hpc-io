from mpi4py import MPI
from azure.storage.blob import BlockBlobService, BlockListType, PageBlobService
from common import common

class AzureBlobBench(object):
	''' 
	Tools for benchmarking Azure Blob\'s performance for HPC purpose.
	MPI is used for process management.

	param:
	 account_name: Azure Storage account name
	 account_key: Azure Storage account key
	 access_container_list: Containers to be accessed
	'''
	__slots__=('__mpi_rank', '__mpi_size', '__block_blob_service', '__page_blob_service')

	def __init__(self, account_name, account_key, access_container_list):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()

		self.__block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
		self.__page_blob_service = PageBlobService(account_name=account_name, account_key=account_key)
		
		if not isinstance(access_container_list, list):
			raise TypeError('access_container_list should be a list!')
		for container in access_container_list:
			self.__block_blob_service.get_container_acl(container)
			self.__page_blob_service.get_container_acl(container)

	def __str__(self):
		return '[AzureBlobBench]: on rank {0} out of {1}'.format(self.__mpi_rank, self.__mpi_size)

	__repr__ = __str__

	def bench_inputs_with_single_block_blob(self, container_name, blob_name, section_limit = 1024):
		'''
		Benchmarking Block Blob get with multiple access on a single blob within a single container.
		
		If the size of source blob is larger than limits, the get operation will be divided into serval sections, with getting size of section_limit each time.
		
		For benchmarking on a single large blob, the input shoud be set to a proper size that can be divided by MPI_SIZE * section_limit

		param:
		 container_name: Blob container
		 blob_name: Blob name
		 section_limit: Limit of sections for each get operation in MiB
		
		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		# Check sections to be get
		blob_size = self.__block_blob_service.get_blob_properties(container_name, blob_name).properties.content_length # in bytes
		blob_size_in_mib = blob_size >> 20 # in MiB
		section_limit_in_bytes = section_limit << 20 # in bytes
		section_count = 0 # get operations to be performed
		if blob_size_in_mib <= section_limit:
			section_count = 1
		else:
			if blob_size_in_mib % self.__mpi_size:
				raise ValueError('blob size cannot be divided by mpi size')
			section_count = blob_size_in_mib // self.__mpi_size
			if section_count % section_limit:
				section_count = section_count // section_limit + 1
			else:
				section_count = section_count // section_limit

		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		if section_count == 1:
			self.__block_blob_service.get_blob_to_bytes(container_name, blob_name)
		else:
			for section in range(0, section_count):
				range_start = section * (self.__mpi_size * section_limit_in_bytes) + self.__mpi_rank * section_limit_in_bytes
				range_end = range_start + section_limit_in_bytes - 1
				if range_start > blob_size - 1:
					break
				if range_end > blob_size - 1:
					range_end = blob_size - 1
				self.__block_blob_service.get_blob_to_bytes(container_name, blob_name, start_range=range_start, end_range=range_end)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_inputs_with_multiple_block_blobs(self, container_name, blob_name, section_limit=1024):
		'''
		Benchmarking Block Blob get with multiple access on multiple processes within a single container.
		Blob size should be valid.
		Blobs corresponding to each processes should be named after the pattern of blob_name + rank

		param:
		 container_name: Blob container
		 blob_name: Blob name
		 section_limit: Limit of sections for each get operation in MiB

		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		proc_blob_name = blob_name + '{:0>5}'.format(self.__mpi_rank)

		# Check sections to be get
		blob_size = self.__block_blob_service.get_blob_properties(container_name, proc_blob_name).properties.content_length # in bytes
		blob_size_in_mib = blob_size >> 20 # in MiB
		section_limit_in_bytes = section_limit << 20 # in bytes
		section_count = blob_size_in_mib // section_limit
		if blob_size_in_mib % section_limit:
			section_count += 1

		# Get
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for section in range(0, section_count):
			range_start = section * section_limit_in_bytes
			range_end = range_start + section_limit_in_bytes - 1
			if range_start > blob_size - 1:
				break
			if range_end > blob_size - 1:
				range_end = blob_size - 1
			self.__block_blob_service.get_blob_to_bytes(container_name, proc_blob_name, start_range=range_start, end_range=range_end)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_bench_metrics(end - start)

	def bench_outputs_with_single_block_blob(self, container_name, blob_name, output_per_rank = 1024, block_limit = 100):
		'''
		Benchmarking Block Blob write with multiple access on a single blob within a single container
		
		Data from different rank is stored in different blocks

		Pattern of global block ids: 00002-00005, first section represents for the rank while second section represents block id written by the rank

		The process is :
		1. Each rank write blocks to Azure
		2. MPI_Barrier() to wait for all ranks
		3. Get uncommited block list, rearrange for order
		4. Commit

		param:
		 container_name: Azure container name
		 blob_name: Blob to be written to
		 output_per_rank: size of outputs per rank, in MiB
		 block_limit: limits of block size, in MiB

		return:
		 max_write_time: maximum writing time
		 min_write_time: minimum writing time
		 avg_write_time: average writing time
		 postprocessing_time: post processing time, composed of step 3 and step 4
		'''
		# Data prepare
		block_limit_in_bytes = block_limit << 20 # to bytes
		data = bytes(self.__mpi_rank for i in range(0, block_limit_in_bytes))
		last_block_data = data
		block_count = output_per_rank // block_limit
		if output_per_rank % block_limit:
			block_count += 1
			last_block_size = (output_per_rank % block_limit) << 20 # in bytes
			last_block_data = bytes(self.__mpi_rank for i in range(0, last_block_size))

		# PUT blocks
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		for i in range(0, block_count):
			block_id = '{:0>5}-{:0>5}'.format(self.__mpi_rank, i)
			if i != (block_count - 1):
				self.__block_blob_service.put_block(container_name, blob_name, data, block_id)
			elif i == (block_count - 1):
				self.__block_blob_service.put_block(container_name, blob_name, last_block_data, block_id)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		max_write, min_write, avg_write = common.collect_bench_metrics(end - start)

		start_postprocessing = 0
		end_postprocessing = 0
		if 0 == self.__mpi_rank:
			start_postprocessing = MPI.Wtime()
			# Get block list to be committed and rearrange
			block_list = self.__block_blob_service.get_block_list(container_name, blob_name, block_list_type=BlockListType.All).uncommitted_blocks
			block_list.sort(key = lambda block:block.id)

			# Commmit
			self.__block_blob_service.put_block_list(container_name, blob_name, block_list)
			end_postprocessing = MPI.Wtime()

		# TODO: Every processes check their own part

		return max_write, min_write, avg_write, end_postprocessing - start_postprocessing

	def bench_outputs_with_multiple_blockblob(self, container_name, blob_name, output_per_rank = 1024, block_limit = 100, data = None):
		'''
		Benchmarking Block Blob write with multiple access on multiple blobs within a single container
		
		Data from different rank is stored in different blobs.

		Pattern of output blobs is: blob_name + 00001 where the second parts represents for the rank of the process 

		Format of global block ids: 00002-00005, first section represents for the rank and second section represents block id written by the rank

		param:
		 container_name: Azure container name
		 blob_name: Blob to be written to
		 output_per_rank: size of outputs per rank, in MiB. Should be able to be divided by section_limit
		 block_limit: limits of blocks, in MiB

		return:
		 max_write: maximum write time
		 min_write: minimum write time 
		 avg_wrtie: average write time
		 max_postprocessing: maximum postprocessing time
		 min_postprocessing: minimum postprocessing time 
		 avg_postprocessing: average postprocessing time
		'''
		# Data prepare
		if output_per_rank > 1025:
			raise ValueError('Not support for large file size currently')
		output_per_rank_in_bytes = output_per_rank << 20 # in bytes
		if data == None:
			data = bytes(self.__mpi_rank for i in range(0, output_per_rank_in_bytes))
		output_blob_name = blob_name + '{:0>5}'.format(self.__mpi_rank)

		# Output
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		self.__block_blob_service.create_blob_from_bytes(container_name, output_blob_name, data)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()
		max_write, min_write, avg_write = common.collect_bench_metrics(end - start)

		return max_write, min_write, avg_write