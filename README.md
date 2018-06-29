# Azure HPC IO
## Overview
Benchmarking tool for I/O performance on Azure Storage for HPC purpose. 

## Usage

## Azure Storage Specifications
### Product Features
| Item | Azure Block Blob | Azure File |
| :------------- |:-------------| :-----|
| Capacity | 500 TiB / container | 5TiB / file share |
| File Size | 4.75 TiB per block blob | 1 TiB per file |
| Throughput | 60 MiB/s or 500 requests/s | 60 MiB/s or 1000 IOPS  |

### Product Prices
Here we specify the cost for **General Purpose v1** accounts on **UK South**. **LRS**(Locally Redundant Storage) is used in our testcase. Further details of other options can be found on [Azure](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/)

| Storage Capacity | Azure Block Blob | Azure File |
| :------ | :-------| :-------|
| 0 ~ 1 TB / month | £0.0224 per GB | £0.056 per GB |
| 1 ~ 50 TB / month |  £0.0220 per GB | £0.056 per GB |
| 50 ~ 500 TB / month |  £0.0217 per GB | £0.056 per GB |
| 500 ~ 1000 TB / month |  £0.0213 per GB | £0.056 per GB |
| 1000 ~ 5000 TB / month |  £0.0209 per GB | £0.056 per GB |

| Operations | Azure Block Blob | Azure File |
| :------ | :-------| :-------|
| Any Operations per 10,000 | £0.00027 | £0.0112 |

## Test 