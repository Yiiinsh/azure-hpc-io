from mpi4py import MPI
import os, configparser
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

	# blob download
	block_blob_service = BlockBlobService(account_name=config['AZURE']['account_name'], account_key=config['AZURE']['account_key'])
	block_blob_service.get_container_acl('iotestblob')
	start = MPI.Wtime()
	block_blob_service.get_blob_to_text('iotestblob', 'hello.c').content
	end = MPI.Wtime()
	print('Time {time} seconds of rank {rank}'.format(time = str(end - start), rank=str(rank)))


if __name__ == '__main__':
	benchblob()