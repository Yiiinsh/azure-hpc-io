from mpi4py import MPI

class BaseBench(object):
	'''
	Base class for benchmarking tools for HPC purpose.
	MPI is used for process management.

	param:
	 access_name: Storage target access name
	 access_key: Storage target access key
	 access_container_list: Containers to be accessed
	'''
	__slots__ = ('__bench_target', '__mpi_rank', '__mpi_size', '__storage_service')
	
	def __init__(self, access_name, access_key, access_container_list):
		self.__bench_target = 'Base'
		self.__mpi_rank = MPI.COMM_WORLD.Get_rank()
		self.__mpi_size = MPI.COMM_WORLD.Get_size()
		self.__storage_service = None

	def __str__(self):
		return '[{0}]: on rank {1} out of {2}'.format(self.__bench_target, self.__mpi_rank, self.__mpi_size)

	__repr__ = __str__

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
		raise NotImplementedError()

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
		raise NotImplementedError()

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
		raise NotImplementedError()

	def bench_outputs_with_single_file_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data):
		'''
		Benchmarking outputs with pattern `Single File Multiple Writers`
		
		Each processes will access a single shared file in different sections exclusively.

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
		raise NotImplementedError()
	
	def bench_outputs_with_multiple_files_multiple_writers(self, container_name, directory_name, file_name, output_per_rank, data):
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
		raise NotImplementedError()

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
		raise NotImplementedError()