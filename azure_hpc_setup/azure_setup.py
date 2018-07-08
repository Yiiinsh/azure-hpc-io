#! /usr/bin/env python3
'''
Script for Azure environment setup & task submission
'''

import configparser
import os
import time
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.file import FileService
from azure.batch.batch_service_client import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodel

# Configurations
config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)
config_azure = config['AZURE']

# Azure
batch_credential = SharedKeyCredentials(
    config_azure['batch_account_name'], config_azure['batch_account_key'])
batch_service = BatchServiceClient(
    batch_credential, config_azure['batch_account_url'])
block_blob_service = BlockBlobService(
    account_name=config_azure['storage_account_name'], account_key=config_azure['storage_account_key'])
file_service = FileService(
    account_name=config_azure['storage_account_name'], account_key=config_azure['storage_account_key'])


def source_file_upload():
    '''
    Upload related source file to Azure Blob
    '''
    source_files = []
    unique_files = []
    for folder, _, files in os.walk('../'):
        # Skip setup folder
        if os.path.abspath(folder) == os.path.abspath('./'):
            continue

        for file_name in files:
            if file_name.endswith('.py') or file_name.endswith('.ini') or file_name == config_azure['source_file_name']:
                if not file_name in unique_files:
                    source_files.append(
                        (os.path.abspath(os.path.join(folder, file_name)), file_name))
                    unique_files.append(file_name)

    # Source Upload
    block_blob_service.create_container(
        config_azure['source_container'], fail_on_exist=False, public_access=PublicAccess.Blob)
    file_service.create_share(config_azure['source_share'])
    for path, file_name in source_files:
        print('Uploading file {} to container [{}]...'.format(
            file_name, config_azure['source_container']))
        block_blob_service.create_blob_from_path(
            config_azure['source_container'], file_name, path)

        if file_name == config_azure['source_file_name']:
            print('Uploading file {} to file share [{}]...'.format(
                file_name, config_azure['source_share']))
            file_service.create_file_from_path(
                config_azure['source_share'], None, file_name, path)


def source_file_cleanup():
    '''
    Clean up source file from Azure
    '''
    print('Deleting container [{0}]...'.format(
        config_azure['source_container']))
    block_blob_service.delete_container(config_azure['source_container'])
    print('Deleting file share [{0}]...'.format(config_azure['source_share']))
    file_service.delete_share(config_azure['source_share'])

def test_file_upload(size = 64 * 1024 * 1024):
	content = '0' * size
	block_blob_service.create_blob_from_text(config_azure['source_container'], config_azure['source_file_name'], content)
	file_service.create_file_from_text(config_azure['source_share'], None, config_azure['source_file_name'], content)

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
		command_line='/bin/bash -c "sudo yum -y install epel-release; sudo yum -y install python36 python36-devel python36-tools; sudo python36 -m ensurepip; sudo yum -y install openmpi openmpi-devel; sudo env MPICC=/usr/lib64/openmpi/bin/mpicc pip3 install mpi4py numpy; sudo pip3 --yes uninstall azure azure-common azure-storage; sudo pip3 install azure-storage"',
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
    batch_service.pool.delete(
		pool_id=config_azure['batch_pool_name']
	)

def job_create():
	batch_service.job.add(
		job=batchmodel.JobAddParameter(
			id=config_azure['job_id'],
			pool_info=batchmodel.PoolInformation(
				pool_id=config_azure['batch_pool_name']
			)
		)
	)

def job_destroy():
	batch_service.job.delete(config_azure['job_id'])

def task_submit():
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
        id='benchtask',
        command_line=command,
        multi_instance_settings=multi_instance_settings,
        user_identity=user
    )

    print('Adding bench tasks to job [{0}]...'.format(config_azure['job_id']))
    batch_service.task.add(config_azure['job_id'], task)


if __name__ == '__main__':
    task_submit()
