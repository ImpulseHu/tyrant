go build
cd shellExecutor
go build
mv -f shellExecutor ../example_executor
cd ../
rm example_executor.tar.gz -f
tar -czf example_executor.tar.gz example_executor
./tyrant --master=zk://localhost:2181/mesos  --conf=./config.ini
