#!/bin/bash
hdfs namenode -format
service ssh start
if [ "$HOSTNAME" = node-hive ]; then
    echo "########## waiting postgresql catalog start"
    sleep 30
    FILE=/root/.metastore
    if [ -f "$FILE" ]; then
        echo "########## $FILE exists - skipping metastore configuration"
    else
        echo "########## configuring postgresql as hive metastore"
        /opt/hive/bin/schematool -dbType postgres -initSchema
        echo "########## persisting .metastore control file"
        echo "hive-metastore" >> "$FILE"
    fi
    echo "########## starting hive as metastore"
    hive --service metastore &
fi
if [ "$HOSTNAME" = node-master ]; then
    start-dfs.sh
    start-yarn.sh
    # start-master.sh
    echo "########## starting hive as query engine"
    nohup hive --service hiveserver2 --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.root.logger=DEBUG,console &
    cd /root/lab
    echo "########## starting jupyter notebook"
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' &
fi
#bash
while :; do :; done & kill -STOP $! && wait $!
