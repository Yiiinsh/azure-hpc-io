# Azure Specifications
## Azure Storage
### Product Features
| Item | Azure Block Blob | Azure File |
| :------------- |:-------------| :-----|
| Capacity | 500 TiB / container | 5TiB / file share |
| File Size | 4.75 TiB per block blob | 1 TiB per file |
| Throughput | 60 MiB/s or 500 requests/s | 60 MiB/s or 1000 IOPS  |
| Block Limit | 100 MiB | N/A |
| Block Size | 50,000 blocks | N/A |
| Open handles | N/A | 2000 |

### Produce Prices
Here we specify the cost of **General Purpose v1** accounts on **UK South**. **LRS** (Locally Redundant Storage) is used as replication strategy. Further details can be found on [Azure Storage Price](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/)


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

### Conclusion
Generally speaking, Azure Block Blob provide larger capacity with lower maintainence and operation costs. Besides Block Blob and File, there are also Azure Page Blob and Append Blob etc that could fit better into some specified scenarios.

## Azure Virtual Machines
For sample experiments, **Azure Standard Av2** series are used. Further informations about other VMs can be found on [Azure Virtual Machines](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)

| Instance | Core | RAM | Temporary Storage | Pay as you Go |
| :----- | :----- | :----- | :----- | :----- |
|  A1 v2 | 1 |  2.00 GiB | 10 GiB | £0.038/hour |
|  A2 v2 | 2 |  4.00 GiB | 20 GiB |  £0.08/hour |
|  A4 v2 | 4 |  8.00 GiB | 40 GiB | £0.166/hour |
|  A8 v2 | 8 | 16.00 GiB | 80 GiB | £0.349/hour |
| A2m v2 | 2 | 16.00 GiB | 20 GiB | £0.133/hour |
| A4m v2 | 4 | 32.00 GiB | 40 GiB | £0.279/hour |
| A8m v2 | 8 | 64.00 GiB | 80 GiB | £0.586/hour |

High performance compute optimized VMs are also available on **H-series**

| Instance | Core | RAM | Temporary Storage | Pay as you Go |
| :----- | :----- | :----- | :----- | :----- | 
|    H8 |  8 |  56.00 GiB | 1000 GiB |  £0.71/hour |
|   H16 | 16 | 112.00 GiB | 2000 GiB | £1.418/hour |
|   H8m |  8 | 112.00 GiB | 1000 GiB |  £0.95/hour |
|  H16m | 16 | 224.00 GiB | 2000 GiB |  £1.90/hour |
| H16mr | 16 | 224.00 GiB | 2000 GiB |  £2.09/hour |
|  H16r | 16 | 112.00 GiB | 2000 GiB |  £1.56/hour |
