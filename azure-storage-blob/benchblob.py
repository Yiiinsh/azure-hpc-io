from mpi4py import MPI
import os, configparser
import numpy
from azure.storage.blob import BlockBlobService

config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)

def benchblob():
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	size = comm.Get_size()
	processor_name = MPI.Get_processor_name()
	print('Hello from rank {rank} of {size}. Processor {processor}'.format(rank = str(rank), size = str(size), processor = processor_name))

	# Blob download
	block_blob_service = BlockBlobService(account_name=config['AZURE']['account_name'], account_key=config['AZURE']['account_key'])
	block_blob_service.get_container_acl(config['AZURE']['source_container_name'])
	
	# Input benchmark
	comm.Barrier()
	start = MPI.Wtime()
	block_blob_service.get_blob_to_text(config['AZURE']['source_container_name'], config['AZURE']['source_blob_name']).content
	end = MPI.Wtime()
	comm.Barrier()

	# Gather metrics
	read_time = numpy.zeros(1)
	max_read_time = numpy.zeros(1)
	min_read_time = numpy.zeros(1)
	avg_read_time = numpy.zeros(1)
	read_time[0] = end - start
	comm.Reduce(read_time, max_read_time, MPI.MAX, 0)
	comm.Reduce(read_time, min_read_time, MPI.MIN, 0)
	comm.Reduce(read_time, avg_read_time, MPI.SUM, 0)
	max_read_time[0] = round(max_read_time[0], 3)
	min_read_time[0] = round(min_read_time[0], 3)
	avg_read_time[0] = round(avg_read_time[0] / size, 3)
	
	if 0 == rank:
		print('Max {max}s, Min {min}s, Avg {avg}s'.format(max = str(max_read_time[0]), min = str(min_read_time[0]), avg = str(avg_read_time[0])))
	


if __name__ == '__main__':
	benchblob()