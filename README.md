# go-distsys-lib
<br>Adapted from http://css.csail.mit.edu/6.824/2014/index.html
<br>The implementation of the project happens in the following sequence:
<br>1) A MapReduce program. 
<br>2) Master that hands out jobs to workers, and handles failures of workers. 
<br>3) primary/backup replication for the key/value database, assisted by a view service that decides which machines are alive. The view services allows the primary/backup service to work correctly in the presence of network partitions.
<br>4) Paxos protocol to replicate the key/value database (fault-tolerent) with no single point of failure, and handling network partitions correctly. 
<br>5) Sharded key/value database, where each shard replicates its state using Paxos. This key/value service can perform Put/Get operations in parallel on different shards, allowing it to support applications such as MapReduce application that can put a high load on a storage system. Lab 4 also has a replicated configuration service, which tells the shards for what key range they are responsible. It can change the assignment of keys to shards, for example, in response to changing load.
