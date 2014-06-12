cd shellExecutor
go build
mv -f shellExecutor ../example_executor
cd ../
rm example_executor.tar.gz -f
tar -czf example_executor.tar.gz example_executor
./tyrant --master=zk://192.168.102.95:2181/mesos 
