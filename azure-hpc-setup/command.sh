/usr/lib64/openmpi/bin/mpirun -mca btl_tcp_if_include eth0 -oversubscribe -n 8 -host $AZ_BATCH_HOST_LIST -wd $AZ_BATCH_TASK_SHARED_DIR python3 $AZ_BATCH_TASK_SHARED_DIR/bench.py