#! /usr/bin/env python3

'''
Data source generator
'''

def generate_source_file(file_name, length, width):
	'''
	Generate source file with specified size.
	Data contained in this file can be parsed to a 2-D array with integer type.
	
	File size will be length * width Bytes.

	Param:
	 file_name : target file name
	 length : length of the simulated data array
	 width : width of the simulated data array
	'''
	data = [0] * length * width
	with open(file_name, 'w') as f:
		f.write(''.join(str(ch) for ch in data))

if __name__ == '__main__':
	generate_source_file('test', 1024, 1024)