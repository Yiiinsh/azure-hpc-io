#! /usr/bin/env python3
'''
Script for Azure environment setup & task submission
'''

import configparser, os, time
from azure.storage import blob, file
from azure.batch.batch_service_client import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from mpi4py import MPI
import azure.batch.models as batchmodel

# Configurations
config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)
config_azure = config['AZURE']

# Azure 
batch_account_name = config_azure['batch_account_name']
batch_account_key = config_azure['batch_account_key']
batch_account_url = config_azure['batch_account_url']
storage_account_name = config_azure['storage_account_name']
storage_account_key = config_azure['storage_account_key']
batch_credential = SharedKeyCredentials(batch_account_name, batch_account_key)
batch_service = BatchServiceClient(batch_credential, batch_account_url)
block_blob_service = blob.BlockBlobService(storage_account_name, storage_account_key)
file_service = file.FileService(storage_account_name, storage_account_key)

source_container = config_azure['source_container']
input_container = config_azure['input_container']

def application_source_upload():
	'''
	Upload related source file to Azure Blob
	'''
	source_files = []
	unique_files = []

	for folder, _, files in os.walk('../'):
		# Skip helper folder
		if os.path.abspath(folder) == os.path.abspath('./'):
			continue
		
		for file_name in files:
			if file_name.endswith('.py') or file_name.endswith('.ini'):
				if not file_name in unique_files:
					source_files.append( (os.path.abspath(os.path.join(folder, file_name)), file_name) )
					unique_files.append(file_name)

	# Application source file upload
	block_blob_service.create_container(source_container, fail_on_exist=False, public_access=blob.PublicAccess.Blob)
	for path, file_name in source_files:
		print('Uploading file {} to container [{}]...'.format(file_name, source_container))
		block_blob_service.create_blob_from_path(source_container, file_name, path)

def application_source_cleanup():
	'''
	Cleanup realted source file from Azure Blob
	'''
	print('Deleting container [{}]...'.format(source_container))
	block_blob_service.delete_container(source_container)

def container_create(container_name, multi = False, count = 0):
	'''
	Create input containers for Azure Blob and Azure File

	Multiple containers will name after container_name + '{:0>5}'.formate(id) where id lies in range(0, count)

	param:
	 container_name: container to be created
	 multi: indicate whether to create multiple containers
	 count: count of multiple containers
	'''
	if multi:
		for i in range(0, count):
			sub_container_name = container_name + '{:0>5}'.format(i)
			print('Creating container [{}]'.format(sub_container_name))
			block_blob_service.create_container(sub_container_name, fail_on_exist=False, public_access=blob.PublicAccess.Blob)
			print('Creating share [{}]'.format(sub_container_name))
			file_service.create_share(sub_container_name)
	else:
		print('Creating container [{}]'.format(container_name))
		block_blob_service.create_container(container_name, fail_on_exist=False, public_access=blob.PublicAccess.Blob)
		print('Creating share [{}]'.format(container_name))
		file_service.create_share(container_name)

def container_destroy(container_name, multi = False, count = 0):
	'''
	Delete containers for Azure Blob and Azure File

	param:
	 container_name: container to be deleted
	 multi: indicate whether to create multiple containers
	 count: count of multiple containers
	'''
	if multi:
		for i in range(0, count):
			sub_container_name = container_name + '{:0>5}'.format(i)
			print('Deleting container [{}]'.format(sub_container_name))
			block_blob_service.delete_container(sub_container_name)
			print('Deleting share [{}]'.format(sub_container_name))
			file_service.delete_share(sub_container_name)
	else:
		print('Deleting container [{}]'.format(container_name))
		block_blob_service.delete_container(container_name)
		print('Deleting share [{}]'.format(container_name))
		file_service.delete_share(container_name)

def pool_create():
	image_reference = batchmodel.ImageReference(
		publisher=config_azure['batch_pool_image_publisher'],
		offer=config_azure['batch_pool_image_offer'],
		sku=config_azure['batch_pool_image_sku']
	)

	vm_config = batchmodel.VirtualMachineConfiguration(
		image_reference=image_reference,
		node_agent_sku_id=config_azure['batch_pool_node_agent_sku']
	)

	vm_start_task = batchmodel.StartTask(
		command_line='/bin/bash -c "sudo yum -y install epel-release; sudo yum -y install python36 python36-devel python36-tools; sudo python36 -m ensurepip; sudo yum -y install openmpi openmpi-devel; sudo env MPICC=/usr/lib64/openmpi/bin/mpicc pip3 install mpi4py numpy; sudo pip3 --yes uninstall azure azure-common azure-storage; sudo pip3 install azure-storage azure-batch"',
		user_identity=batchmodel.UserIdentity(
			auto_user=batchmodel.AutoUserSpecification(
				scope=batchmodel.AutoUserScope.pool,
				elevation_level=batchmodel.ElevationLevel.admin
			)
		),
		wait_for_success=True
	)
	
	batch_service.pool.add(
		pool = batchmodel.PoolAddParameter(
        	id=config_azure['batch_pool_name'],
        	vm_size=config_azure['batch_pool_vm_size'],
        	virtual_machine_configuration=vm_config,
			target_dedicated_nodes=config_azure['batch_pool_target_dedicated_nodes'],
			enable_inter_node_communication=True,
			start_task=vm_start_task
		),
		raw=True
	)

def pool_destory():
	batch_service.pool.delete(config_azure['batch_pool_name'])

def job_create():
	batch_service.job.add(
		job = batchmodel.JobAddParameter(
			id=config_azure['job_id'],
			pool_info = batchmodel.PoolInformation(
				pool_id = config_azure['batch_pool_name']
			)
		)
	)

def job_destroy():
	batch_service.job.delete(config_azure['job_id'])

def env_prepare():
	container_create(config_azure['source_container'])
	container_create(config_azure['input_container'])
	container_create(config_azure['output_container'])
	container_create(config_azure['input_container'], True, int(config_azure['task_number_of_procs']))
	container_create(config_azure['output_container'], True, int(config_azure['task_number_of_procs']))
	application_source_upload()
	pool_create()
	job_create()

def env_destroy():
	container_destroy(config_azure['source_container'])
	container_destroy(config_azure['input_container'])
	container_destroy(config_azure['output_container'])
	container_destroy(config_azure['input_container'], True, int(config_azure['task_number_of_procs']))
	container_destroy(config_azure['output_container'], True, int(config_azure['task_number_of_procs']))
	pool_destory()
	job_destroy()

def task_submit(task_name):
    '''
    Automatic task submission to Azure. Pool, VMs, Jobs should be created in advance.
    '''
    common_resource_files = []
    for folder, _, files in os.walk('../'):
        # Skip setup folder
        if os.path.abspath(folder) == os.path.abspath('./'):
            continue

        for file_name in files:
            if file_name.endswith('.py') or file_name.endswith('.ini'):
                blob_url = os.path.join(
                    config_azure['storage_account_url'], file_name)
                file_path = os.path.join(os.path.basename(folder), file_name)
                print('Mapping {} to {}'.format(blob_url, file_path))
                common_resource_files.append(batchmodel.ResourceFile(
                    blob_url, file_path, file_mode='0775'))

    command = '/usr/lib64/openmpi/bin/mpirun -mca btl_tcp_if_include eth0 -oversubscribe -n {0} -host $AZ_BATCH_HOST_LIST -wd $AZ_BATCH_TASK_SHARED_DIR python36 $AZ_BATCH_TASK_SHARED_DIR/bench.py'.format(
        config_azure['task_number_of_procs'])
    coordination_command = '/bin/bash -c "echo $AZ_BATCH_HOST_LIST; echo $AZ_BATCH_TASK_SHARED_DIR; echo $AZ_BATCH_MASTER_NODE;"'
    multi_instance_settings = batchmodel.MultiInstanceSettings(
        coordination_command_line=coordination_command,
        number_of_instances=config_azure['task_number_of_instances'],
        common_resource_files=common_resource_files
    )
    user = batchmodel.UserIdentity(auto_user=batchmodel.AutoUserSpecification(
        scope=batchmodel.AutoUserScope.pool,
        elevation_level=batchmodel.ElevationLevel.non_admin
    ))
    task = batchmodel.TaskAddParameter(
        id=task_name,
        command_line=command,
        multi_instance_settings=multi_instance_settings,
        user_identity=user
    )

    print('Adding bench tasks to job [{0}]...'.format(config_azure['job_id']))
    batch_service.task.add(config_azure['job_id'], task)

def input_blob_upload(blob_name = 'test', blob_size = 1024 * 1024 * 1, multiple_blob = False, multiple_container = False, count = 0):
	content = bytes(0 for i in range(0, blob_size))

	if multiple_blob:
		if multiple_container:
			for i in range(0, count):
				sub_container_name = input_container + '{:0>5}'.format(i)
				sub_blob_name = blob_name +  '{:0>5}'.format(i)
				print('Upload blob {0} with size of {1} to {2}'.format(sub_blob_name, blob_size, sub_container_name))
				block_blob_service.create_blob_from_bytes(sub_container_name, sub_blob_name, content)
		else:
			for i in range(0, count):
				sub_blob_name = blob_name +  '{:0>5}'.format(i)
				print('Upload blob {0} with size of {1} to {2}'.format(sub_blob_name, blob_size, input_container))
				block_blob_service.create_blob_from_bytes(input_container, sub_blob_name, content)
	else:
		print('Upload blob {0} with size of {1} to {2}'.format(blob_name, blob_size, input_container))
		block_blob_service.create_blob_from_bytes(input_container, blob_name, content)	

def input_file_upload(file_name = 'test', file_size = 1024 * 1024 * 1, multiple_file = False, multiple_contaienr = False, count = 0):
	content = '0' * file_size
	content = bytes(content, 'utf-8')

	if multiple_file:
		if multiple_contaienr:
			for i in range(0, count):
				sub_container_name = input_container + '{:0>5}'.format(i)
				sub_file_name = file_name + '{:0>5}'.format(i)
				print('Upload file {0} with size of {1} to {2}'.format(sub_file_name, file_size, sub_container_name))		
				file_service.create_file_from_bytes(sub_container_name, None, sub_file_name, content)
		else:
			for i in range(0, count):
				sub_file_name = file_name + '{:0>5}'.format(i)
				print('Upload file {0} with size of {1} to {2}'.format(sub_file_name, file_size, input_container))		
				file_service.create_file_from_bytes(input_container, None, sub_file_name, content)
	else:
		print('Upload file {0} with size of {1} to {2}'.format(file_name, file_size, input_container))
		file_service.create_file_from_bytes(input_container, None, file_name, content)

def large_input_blob_upload(blob_name = 'test', inputs_per_rank = 1024 * 25):
    '''
    Integrated with MPI to upload large inputs.
    Inputs per rank should be able to divided by block limit

    param:
     blob_name: name of blob to be created
     inputs_per_rank: size of inpus for each rank, measure in MiB

    '''
    rank = MPI.COMM_WORLD.Get_rank()
    block_limit = 100 # in MiB
    
    section_size = inputs_per_rank // block_limit
    if inputs_per_rank % block_limit:
        section_size += 1
    
    
    data = '0' * (block_limit << 20)
    data = bytes(data, 'utf-8')
    if inputs_per_rank % block_limit:
        last_data = '0' * ((inputs_per_rank % block_limit) << 20 )
        last_data = bytes(last_data, 'utf-8')
    else:
        last_data = data

    MPI.COMM_WORLD.Barrier()
    for i in range(0, section_size):
        block_id = '{:0>5}-{:0>5}'.format(rank, i)
        print('uploading block {0}'.format(block_id))
        if i == section_size - 1:
            block_blob_service.put_block(config_azure['input_container_name'], blob_name, last_data, block_id)
        else:
            block_blob_service.put_block(config_azure['input_container_name'], blob_name, data, block_id)
    MPI.COMM_WORLD.Barrier()

    if 0 == rank:
        block_list = block_blob_service.get_block_list(config_azure['input_container_name'], blob_name, block_list_type=blob.BlockListType.All).uncommitted_blocks
        block_blob_service.put_block_list(config_azure['input_container_name'], blob_name, block_list)

if __name__ == '__main__':
    large_input_blob_upload()
