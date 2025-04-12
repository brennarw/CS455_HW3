# Distributing Systems HW3

## OBJECTIVE
The objective of this assignment is to gain experience in developing MapReduce programs. As part of this assignment, you will be working with the Million Song dataset. You will be developing MapReduce programs that parse and process song analyses and metadata to answer questions about the songs.
You will be using Apache Hadoop (version 3.3.6) to implement this assignment. 

## Cluster setup
As part of this assignment you are responsible for setting up your own Hadoop cluster with HDFS running on every node. We will be staging datasets on a read-only cluster. You should use your own cluster to write outputs produced by your MapReduce programs. MapReduce clients will be able to access namespaces of both clusters through Hadoop ViewFS federation. Your programs will process the staged datasets; data locality will be preserved by the MapReduce runtime.
