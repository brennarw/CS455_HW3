NameNode: lincoln
DataNodes: nashville, olympia, jefferson-city

how to start hadoop cluster:
    $HADOOP_HOME/sbin/start-dfs.sh

how to stop hadoop cluster:
    $HADOOP_HOME/sbin/stop-dfs.sh

how to list directories in the hadoop cluster:
    $HADOOP_HOME/bin/hadoop fs -ls / 
    (list specific directories after the dash if you want to see specific files)

how to make a specific directory called "wc":
    $HADOOP_HOME/bin/hadoop fs -mkdir /wc

how to put a text file in the "wc" directory:
    $HADOOP_HOME/bin/hadoop fs -put youTXTFilename /wc

how to execute the jar for this program specifically (run the mappers and reducers on the textfile):
    hadoop jar 455Repositories/CS455_HW3/build/libs/CS455_HW3-1.0-SNAPSHOT.jar csx55.hadoop.MapReduce /hw3DataSets /hw3Output
    (hw3Output will be put in the "output" file)
    (MapReduce is where you main entry point is)