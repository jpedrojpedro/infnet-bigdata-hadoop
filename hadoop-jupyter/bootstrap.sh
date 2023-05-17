#!/bin/bash
hdfs namenode -format
service ssh start
if [ "$HOSTNAME" = node-hive ]; then
    echo "########## waiting postgresql catalog start"
    sleep 15
    echo "########## configuring postgresql as hive metastore"
    /opt/hive/bin/schematool -dbType postgres -initSchema
    echo "########## starting hive as metastore"
    hive --service metastore &
fi
if [ "$HOSTNAME" = node-master ]; then
    start-dfs.sh
    start-yarn.sh
    # start-master.sh
    echo "########## waiting hive catalog start"
    sleep 30
    echo "########## starting hive as query engine"
    hive --service hiveserver2 --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
                               --hiveconf hive.server2.thrift.http.port=10001 \
                               --hiveconf hive.server2.thrift.http.path=cliservice \
                               --hiveconf hive.server2.authentication=NONE \
                               --hiveconf hive.server2.transport.mode=http \
                               --hiveconf hive.server2.active.passive.ha.enable=true \
                               --hiveconf hive.server2.enable.doAs=false \
                               --hiveconf hive.metastore.uris="thrift://node-hive:9083" \
                               --hiveconf hive.root.logger=DEBUG,console &
    cd /root/lab
    echo "########## starting jupyter notebook"
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
                     --NotebookApp.token='' \
                     --NotebookApp.password='' &
fi
#bash
while :; do :; done & kill -STOP $! && wait $!
