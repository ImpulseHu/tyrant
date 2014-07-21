#!/bin/sh
go build
chmod +x tyrant
cd shellExecutor
go build
chmod +x shellExecutor
mv -f shellExecutor ../shell_executor
cd ../
rm shell_executor.tar.gz -f
tar -czf shell_executor.tar.gz shell_executor
./tyrant --master=zk://localhost:2181/mesos  --conf=./config.ini
