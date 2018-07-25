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

## Results
### Input
Details can be found on [Input Bench](doc/INPUT.md)

### Output
Details can be found on [Output Bench](doc/OUTPUT.md)