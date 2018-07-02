#! /usr/bin/env python3
'''
Script for Azure environment setup & task submission
'''

import configparser, os
from azure.storage.blob import BlockBlobService

config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)
config_azure = config['AZURE']

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
	block_blob_service.create_container(config_azure['input_container'], fail_on_exist=False)
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
	pass
