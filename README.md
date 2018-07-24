# Azure HPC I/O
## Overview
Tools for investigating high performance parallel I/O on Azure's cloud-native storage 

## Project Structure

## Usage
```
mpirun -n [number of procs] python3 bench.py
```

**Note**: all the related resources on Azure should be set up in advance.

## Specifications
### Azure Spec
Related Azure specifications can be found on [Azure Spec](doc/AzureSpec.md)

### Execution Environments
| Dependency | Cirrus | Azure VMs |
| :------ | :-------| :-------|
| Python | 3.6.4 | 3.6.3 |
| MPI | HPE MPT 2.16 | Open MPI 1.10.7 |
| mpi4py | 3.0.0 | 3.0.0 |
| numpy | 1.14.0 | 1.14.5 |
| azure-common | 1.1.12 | 1.1.13 |
| azure-storage | 0.36.0 | 0.36.0 |

## Input
Details can be found on [Input Bench](doc/INPUT.md)

## Output
### Overview
This section presents the result on outputs benchmarking. 'Single file, Multiple Writers' together with 'Multiple files, multiple writers' pattern are used in this experiment and each process will update an individual single section of the whole file. Sections is divided evenly across processes. Results for running on a lustre file system within Cirrus are also attached.

### File Size
Experiments run on **5 Nodes, 4 processes** each.

#### Single File, Multiple Writers
Demonstrations on how to do SFMW pattern on Azure can be found in [Blob](doc/img/blobsfmw.jpg) and [File](doc/img/filesfmw.jpg)

| Size(MiB) per Proc | Total Size(MiB) | Blob(MiB/s) | File(MiB/s) | Cirrus(MiB/s) |
| :------ | :-------| :-------| :-------| :-------|
| 1    |    20 | 111.111 | 133.333 |  |
| 2    |    40 | 205.128 | 174.672 |  |
| 4    |    80 | 255.591 | 249.221 |  |
| 8    |   160 | 330.579 | 325.203 |  |
| 16   |   320 | 397.022 | 308.285 |  |
| 32   |   640 | 385.775 | 259.004 |  |
| 64   |  1280 | 448.179 | 277.838 |  |
| 128  |  2560 | 444.290 | 311.473 |  |
| 256  |  5120 | 475.483 | 291.854 |  |
| 512  | 10240 | 515.039 | 340.143 |  |

#### Multiple Files, Multiple Writers
| Size(MiB) per Proc | Total Size(MiB) | Blob(MiB/s) | File(MiB/s) | Cirrus(MiB/s) |
| :------ | :-------| :-------| :-------| :-------|
| 1    |     20 | 243.902 | 139.860 |  5000.000 |
| 2    |     40 | 231.214 | 132.450 | 10000.000 |
| 4    |     80 | 325.203 | 188.679 | 11428.571 |
| 8    |    160 | 441.989 | 336.842 |  5714.286 |
| 16   |    320 | 415.045 | 376.028 |  2388.060 |
| 32   |    640 | 390.959 | 343.164 |  2549.801 |
| 64   |   1280 | 475.836 | 360.259 |  3350.785 |
| 128  |   2560 | 500.685 | 393.060 |  2952.710 |
| 256  |   5120 | 492.402 | 371.742 |  3017.089 |
| 512  |  10240 | 515.039 | 408.962 |  2833.426 |

#### Trends on Azure
![Write Trends](doc/img/WriteTrend.jpg)

### Nodes
Experiments run on **20** processes with file size fixed at 64 MiB.

#### Bandwidth

## Number of processes
TBD.

### Potential Improvments
Compression & Metadata for management

### Analysis