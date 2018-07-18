# Input Benchmarking
## Background
For a traditional HPC applications hosted on dedicated supercomputers such as [Cirrus](https://www.epcc.ed.ac.uk/facilities/demand-computing/cirrus) or [Archer](http://www.archer.ac.uk/), normally we have the I/O hierachy as is shown is the figure. However, if we want to migrate our HPC code to host in a cloud-native environments and interact with cloud-native storage, it is not easy to utilize the framework enhancements on I/O (e.g MPI's collective I/O). So, similar to directly use POSIX I/O for inputs & outputs, we use cloud APIs to access the cloud storage.

![IOCharacterization](img/IOCharacterization.jpg)

## Strategy
Since we target at cloud-native storage such as web-based object storage or file storage, by input we mean that download the data via HTTP and load them into our VM's memory for further processing. For the inputs, we assume that the source data can be evenly divided and each processes accounts for processing one distinct sections at a time. Based on these conditions, we introduce the following input patterns.

### Single Reader & Broadcast
Assume that we are using MPI framework for process managements. Process at rank 0 will download all the data into its memory and then it will distribute sections of data to each processes accordingly.

![MasterReadAndBroadcast](img/MasterReadAndBroadcast.jpg)

Restricted by the available memory for cloud VMs, the master process can only download a specified amount of data. Besides, the interconnect within HPC clusters deployed in public clouds is not as fast as in a supercomputer. So, generally, this strategy is not well suited for a cloud native HPC solution.

### Singel File, Multiple Readers
Every process reads a range of or the entire data from a shared file.

![SingleFileMultipleReaders](img/SingleFileMultipleReaders.jpg)

Cloud provide range get APIs which could help in applying this pattern.

### Multiple Files, Multiple Readers
Source data has been originally present or pre-processing into serval different files. Each process reads their corresponding files. 

![MultipleFilesMultipleReaders](img/MultipleFilesMultipleReaders.jpg)

This strategy requires source data to be pre-processed and well matched with the amount of processes we are going to use. However, since there are no corruptions between files, this strategy is well parallelized.

## Conditions
* The application are run with one process per core
* The amount of data each process downloads is restricted by the available memory
* The limitation of a single Azure Blob is 4.75 TiB and Azure File is 1 TiB
* Block Blob is used for Blob benchmarking and default block size are used

## Benchmark
This section reveals the performance of 'Single File, Multiple Readers' and 'Multiple Files, Multiple Readers' pattern using Azure's cloud-native storage. Reading from Azure Blob, Azure File and corresponding operations using POSIX I/O on Cirrus Lustre file system are performed and results are listed.

Experiments run on **5 nodes, 4 processes** each. Azure Standard **A4_v2** VM configurations are used. Results got from 100 iterations of benchmarkings.

For 'Multiple Files, Multiple Readers' pattern, files are evenly divided into corresponding sub-files. E.g. 100 GiB file, 20 processes, we divide it into 20 small files with 5 GiB each. Each processes reads their own file.

For all I/O operations, there are limits on data sizes for each operation. If the size of data exceeds the limits, the operation will be sepearated into serval sub-ops that make up the entire operation. For instance, if we want to update a 4 GiB file with the limit of 1GiB, the operation will be divided into 4 sub-ops that upload 1 GiB each time.

### Start Up
As is restricted by the VM's memory, for each downloads we can only get the inputs less than 1.5 GiB on **A4_v2**. For start up, we test the case that all the processes downloads the entire file with the size that could be directly loaded into memory. Demonstrations for this pattern can be found below:

![InputStartup](img/InputStartup.jpg)

#### Single File, Multiple Readers
| File Size(MiB) | Blob Latency (s) | Blob Bandwidth (MiB/s) | File Latency (s) | File Bandwidth (MiB/s) | Cirrus Latency (s) | Cirrus Bandwidth (MiB/s) |
| :------ | :-------| :-------| :-------| :-------| :-------| :-------|
|    1 | 0 | 0 | 0 | 0 | 0 | 0 |  
|    2 | 0 | 0 | 0 | 0 | 0 | 0 |
|    4 | 0 | 0 | 0 | 0 | 0 | 0 |
|    8 | 0 | 0 | 0 | 0 | 0 | 0 |
|   16 | 0 | 0 | 0 | 0 | 0 | 0 |
|   32 | 0 | 0 | 0 | 0 | 0 | 0 |
|   64 | 0 | 0 | 0 | 0 | 0 | 0 |
|  128 | 0 | 0 | 0 | 0 | 0 | 0 |
|  256 | 0 | 0 | 0 | 0 | 0 | 0 |
|  512 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1024 | 0 | 0 | 0 | 0 | 0 | 0 |

#### Multiple Files, Multiple Readers
| File Size(MiB) | Blob Latency (s) | Blob Bandwidth (MiB/s) | File Latency (s) | File Bandwidth (MiB/s) | Cirrus Latency (s) | Cirrus Bandwidth (MiB/s) |
| :------ | :-------| :-------| :-------| :-------| :-------| :-------|
|    1 | 0 | 0 | 0 | 0 | 0 | 0 |  
|    2 | 0 | 0 | 0 | 0 | 0 | 0 |
|    4 | 0 | 0 | 0 | 0 | 0 | 0 |
|    8 | 0 | 0 | 0 | 0 | 0 | 0 |
|   16 | 0 | 0 | 0 | 0 | 0 | 0 |
|   32 | 0 | 0 | 0 | 0 | 0 | 0 |
|   64 | 0 | 0 | 0 | 0 | 0 | 0 |
|  128 | 0 | 0 | 0 | 0 | 0 | 0 |
|  256 | 0 | 0 | 0 | 0 | 0 | 0 |
|  512 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1024 | 0 | 0 | 0 | 0 | 0 | 0 |	

### Extended to Larger File Sizes
This section reveals the performance of getting a large single file from the cloud. As is limited by the available memory size of host VMs, for a large inputs it is impossible to download the entire file at once. So the source file is evenly divided into serval sub-sections and each processes is responsible for one of them. (e.g for a 500 GiB file with 20 processes processing on it, each process will work on its 25 GiB sub-section) Demonstrations for this pattern is represented below:

![InputExtend](img/InputExtend.jpg)

| File Size(GiB) | Blob Latency (s) | Blob Bandwidth (GiB/s) | File Latency (s) | File Bandwidth (GiB/s) | Cirrus Latency (s) | Cirrus Bandwidth (GiB/s) |
| :------ | :-------| :-------| :-------| :-------| :-------| :-------|
| 500             | 0 | 0 | 0 | 0 | 0 | 0 |  
| 1000            | 0 | 0 | 0 | 0 | 0 | 0 |  
| 2560(~2.5 TiB)  | 0 | 0 | 0 | 0 | 0 | 0 |
| 4860(~4.75 TiB) | 0 | 0 | 0 | 0 | 0 | 0 |