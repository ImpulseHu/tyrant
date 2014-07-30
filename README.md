Tyrant
======
works on mesos 0.18.2+

[![Build Status](https://drone.io/github.com/ngaut/tyrant/status.png)](https://drone.io/github.com/ngaut/tyrant/latest)
[![Coverage Status](https://coveralls.io/repos/ngaut/tyrant/badge.png?branch=master)](https://coveralls.io/r/ngaut/tyrant)


## Features:
* High availability by master and standby
* Simple and clean web ui
* Restful api support
* No need to write mesos executor, just write shell scripts
* Killing tasks
* Realtime task status
* Support webhook 
* Authentication by ldap 


## Usage:
		1. start mesos master and slaves. and zookeeper
		   zkServer.sh start
		   mesos-master.sh --zk=zk://host:port/mesos
		   mesos-slave.sh --master=zk://host:port/mesos

		2. ./tyrant --master=zk://host:port/mesos

		3. open browser http://localhost:9090/v2/job

## Api description
		1. Create job
			post to url: /job, body is json object
			
			{
				"name"`           //string 512, unique, must exist
				"executor"`       //string 4096, must exist
				"executor_flags"` //string 4096, must exist
				"owner"`          //string, must exist
				"uris"`           //string 2048
				"schedule"`       //string 2048, can be empty
				"hook"`           //string, can be empty
			}
			
			return:
				full job description(json format)

		2. Run job
			post to url: /job/run/:jobId, jobId is return when you create job
			
			return:
				task description(json format)
				{
				    data:taskId       	    //string, task id
				    ret:					//ret code, 0 mean ok
				}



## Job List View:

![Snapshot1](https://raw.githubusercontent.com/ngaut/tyrant/master/docs/snapshot/snapshot-1.png)

## Task Running Status View:

![Snapshot2](https://raw.githubusercontent.com/ngaut/tyrant/master/docs/snapshot/snapshot-3.png)




	
## LICENSE

	Tyrant is distributed under the terms of the MIT License. 

