#! /usr/bin/env python3
'''
Script for Azure environment setup & task submission
'''

import configparser, os, time
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.batch.batch_service_client import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import TaskAddParameter, MultiInstanceSettings, ResourceFile, UserIdentity, AutoUserScope, ElevationLevel, AutoUserSpecification

config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)
config_azure = config['AZURE']

batch_credential = SharedKeyCredentials(config_azure['batch_account_name'], config_azure['batch_account_key'])
batch_service = BatchServiceClient(batch_credential, config_azure['batch_account_url'])
block_blob_service = BlockBlobService(account_name=config_azure['storage_account_name'], account_key=config_azure['storage_account_key'])

def source_file_upload():
	'''
	Upload related source file to Azure Blob
	'''
	source_files = []
	unique_files = []
	for folder, _, files in os.walk('../'):
		if os.path.abspath(folder) == os.path.abspath('./'):
			# Skip setup folder
			continue

		for file_name in files:
			if file_name.endswith('.py') or file_name.endswith('.ini') or file_name == config_azure['source_file_name']:
				if not file_name in unique_files:
					source_files.append( (os.path.abspath(os.path.join(folder, file_name)), file_name) )
					unique_files.append(file_name)

	# Source File upload
	block_blob_service.create_container(config_azure['input_container'], fail_on_exist=False, public_access=PublicAccess.Blob)
	for path, blob_name in source_files:
		print('Uploading file {} to container [{}]...'.format(blob_name, config_azure['input_container']))
		block_blob_service.create_blob_from_path(config_azure['input_container'], blob_name, path)

def source_file_cleanup():
	'''
	Clean up source file from Azure
	'''
	print('Deleting container [{0}]...'.format(config_azure['input_container']))
	block_blob_service.delete_container(config_azure['input_container'])
	

def task_submission():
	'''
	Automatic task submisson to Azure. Pool, VMs, Jobs should be created in advance.
	'''
	common_resource_files = [] 
	for folder, _, files in os.walk('../'):
		if os.path.abspath(folder) == os.path.abspath('./'):
			# Skip setup folder
			continue

		for file_name in files:
			if file_name.endswith('.py') or file_name.endswith('.ini'):
				blob_url = os.path.join(config_azure['storage_account_url'], file_name)
				file_path = os.path.join(os.path.basename(folder), file_name)
				print('Mapping {} to {}'.format(blob_url, file_path))
				common_resource_files.append(ResourceFile(blob_url, file_path, file_mode='0775'))

	multi_instance_settings = MultiInstanceSettings(
		coordination_command_line='echo',
		number_of_instances=config_azure['task_number_of_instances'],
		common_resource_files=common_resource_files
	)
	user = UserIdentity(auto_user=AutoUserSpecification(
		scope=AutoUserScope.pool,
		elevation_level=ElevationLevel.non_admin
	))
	task = TaskAddParameter(
		id = 'benchtask',
		command_line='/usr/lib64/openmpi/bin/mpirun -mca btl_tcp_if_include eth0 -oversubscribe -n {0} -host $AZ_BATCH_HOST_LIST -wd $AZ_BATCH_TASK_SHARED_DIR python3 $AZ_BATCH_TASK_SHARED_DIR/bench.py'.format(config_azure['task_number_of_procs']),
		multi_instance_settings= multi_instance_settings,
		user_identity=user
	)

	print('Deleting previous benchtask on job [{0}]...'.format(config_azure['job_id']))
	batch_service.task.delete(config_azure['job_id'], 'benchtask')
	print('Adding bench tasks to job [{0}]...'.format(config_azure['job_id']))
	batch_service.task.add(config_azure['job_id'], task)

if __name__ == '__main__':
	task_submission()