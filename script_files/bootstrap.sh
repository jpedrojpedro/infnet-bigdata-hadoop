#!/bin/bash

hdfs namenode -format
service ssh start
if [ "$HOSTNAME" = node-master ]; then
    start-dfs.sh
    start-yarn.sh
    # start-master.sh
    # /opt/hive/bin/schematool -dbType postgres -initSchema
    # hive --service hiveserver2 &
    hive --service metastore &
    cd /root/lab
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' &
fi
#bash
while :; do :; done & kill -STOP $! && wait $!
