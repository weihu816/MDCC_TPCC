How to run TPCC benchmark in MDCC System on three nodes.
======
1. Zookeeper configure
---
copy zk0.cfg/zk1.cfg/zk2.cfg in the conf folder, modify configuration of zookeeper

	tickTime=2000
	clientPort=30110
	initLimit=5
	syncLimit=2
	dataDir=/Users/apple/Desktop/workspace/mdcc/conf/db/zk0
	server.0=[???]:10110:20110
	server.1=[???]:10110:20110
	server.2=[???]:10110:20110

For node 1,
$ bin/zkServer.sh start zk1.cfg
For node 2,
$ bin/zkServer.sh start zk2.cfg
For node 3,
$ bin/zkServer.sh start zk3.cfg

1. AppServer/Storage configure
---
For example:
If we have, the content of <app-server.properties> as follows

mdcc.server.0.0=[???]:9090
mdcc.server.0.1=[???]:9091
mdcc.server.0.2=[???]:9092

mdcc.app.server=localhost:9190

and the content of <mdcc.properties> as follows

mdcc.server.0=localhost:9090
mdcc.server.1=localhost:9091
mdcc.server.2=localhost:9092

and the content of <tpcc.properties> as follows
warehouse=1
runtime=60

Start StorageNode
---
	start the first node 	<StorageNode.java> on port 	9090	i.e. mdcc.my.id = 0
	start the second node 	<StorageNode.java> on port 	9091	i.e. mdcc.my.id = 1
	start the third node 	<StorageNode.java> on port 	9092	i.e. mdcc.my.id = 2

	给每个节点起Server mdcc.my.id 来表示起哪个节点(local)

	$ java -classpath core/target/mdcc-tpcc-1.0.jar:lib/* edu.ucsb.cs.mdcc.paxos.StorageNode

	
Start appServer
---
java -classpath core/target/mdcc-tpcc-1.0.jar:lib/* edu.ucsb.cs.mdcc.paxos.AppServer

Run <TPCCTest.java>
---
java -classpath core/target/mdcc-tpcc-1.0.jar:lib/* edu.ucsb.cs.mdcc.paxos.StorageNode
[time in seconds] 在tpcc.properties 里面设置 单位是秒
[number of warehouses] 在tpcc.properties 里面设置 单位是个
warehouse=1
runtime=60

Result
---
the number of a mix of TPCC transactions running in the given time
Example output:
Running TPCC Transactions for 60 seconds
==============================Result==============================
1565 Transactions finished in 60 seconds
==================================================================
