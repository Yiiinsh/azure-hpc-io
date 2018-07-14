from mpi4py import MPI
from azure.storage.blob import BlockBlobService
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
	__slots__=('__mpi_rank', '__mpi_size', '__block_blob_service')

	def __init__(self, account_name, account_key, access_container_list):
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()

		self.__block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
		
		if not isinstance(access_container_list, list):
			raise TypeError('access_container_list should be a list!')
		for container in access_container_list:
			self.__block_blob_service.get_container_acl(container)

	def __str__(self):
		return '[AzureBlobBench]: on rank {0} out of {1}'.format(self.__mpi_rank, self.__mpi_size)

	__repr__ = __str__

	def bench_inputs_with_single_blob(self, container_name, blob_name):
		'''
		Benchmarking Blob get with multiple access on a single blob within a single container.
		Blob size should be loadable.

		param:
		 container_name: Blob container
		 blob_name: Blob name
		
		return:
		 max_read: maximum read time
		 min_read: minimum read time
		 avg_read: average read time
		'''
		MPI.COMM_WORLD.Barrier()
		start = MPI.Wtime()
		self.__block_blob_service.get_blob_to_bytes(container_name, blob_name)
		end = MPI.Wtime()
		MPI.COMM_WORLD.Barrier()

		return common.collect_input_bench_metrics(end - start)
