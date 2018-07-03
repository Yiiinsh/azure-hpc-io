#! /usr/bin/env python3
'''
Script for Azure environment setup & task submission
'''

import configparser, os, time
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.file import FileService
from azure.batch.batch_service_client import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import TaskAddParameter, MultiInstanceSettings, ResourceFile, UserIdentity, AutoUserScope, ElevationLevel, AutoUserSpecification

# Configurations
config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)
config_azure = config['AZURE']

# Azure
batch_credential = SharedKeyCredentials(config_azure['batch_account_name'], config_azure['batch_account_key'])
batch_service = BatchServiceClient(batch_credential, config_azure['batch_account_url'])
block_blob_service = BlockBlobService(account_name=config_azure['storage_account_name'], account_key=config_azure['storage_account_key'])
file_service = FileService(account_name=config_azure['storage_account_name'], account_key=config_azure['storage_account_key'])

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
					source_files.append( (os.path.abspath(os.path.join(folder, file_name)), file_name) )
					unique_files.append(file_name)

	# Source Upload
	block_blob_service.create_container(config_azure['source_container'], fail_on_exist=False, public_access=PublicAccess.Blob)
	for path, file_name in source_files:
		print('Uploading file {} to container [{}]...'.format(file_name, config_azure['source_container']))
		block_blob_service.create_blob_from_path(config_azure['source_container'], file_name, path)

		if file_name == config_azure['source_file_name']:
			print('Uploading file {} to file share [{}]...'.format(file_name, config_azure['source_share']))
			file_service.create_file_from_path(config_azure['source_share'], None, file_name, path)

def source_file_cleanup():
	'''
	Clean up source file from Azure
	'''
	print('Deleting container [{0}]...'.format(config_azure['source_container']))
	block_blob_service.delete_container(config_azure['source_container'])
	print('Deleting file share [{0}]...'.format(config_azure['source_share']))
	file_service.delete_share(config_azure['source_share'])
	

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
				blob_url = os.path.join(config_azure['storage_account_url'], file_name)
				file_path = os.path.join(os.path.basename(folder), file_name)
				print('Mapping {} to {}'.format(blob_url, file_path))
				common_resource_files.append(ResourceFile(blob_url, file_path, file_mode='0775'))

	command = '/usr/lib64/openmpi/bin/mpirun -mca btl_tcp_if_include eth0 -oversubscribe -n {0} -host $AZ_BATCH_HOST_LIST -wd $AZ_BATCH_TASK_SHARED_DIR python3 $AZ_BATCH_TASK_SHARED_DIR/bench.py'.format(config_azure['task_number_of_procs'])
	coordination_command = 'echo'
	multi_instance_settings = MultiInstanceSettings(
		coordination_command_line=coordination_command,
		number_of_instances=config_azure['task_number_of_instances'],
		common_resource_files=common_resource_files
	)
	user = UserIdentity(auto_user=AutoUserSpecification(
		scope=AutoUserScope.pool,
		elevation_level=ElevationLevel.non_admin
	))
	task = TaskAddParameter(
		id = 'benchtask',
		command_line=command,
		multi_instance_settings=multi_instance_settings,
		user_identity=user
	)

	print('Deleting previous benchtask on job [{0}]...'.format(config_azure['job_id']))
	batch_service.task.delete(config_azure['job_id'], 'benchtask')
	print('Adding bench tasks to job [{0}]...'.format(config_azure['job_id']))
	batch_service.task.add(config_azure['job_id'], task)

if __name__ == '__main__':
	task_submit()