1.Draw the data file.txt and totalnumber.jar into src directory.
2.Copy dile from NFS to HDFS:
hadoop fs -copyFromLocal data.txt ~
3.Set the directory that contains the totalnumber.jar file: 
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/comp/$USER/src/* 
4.Run the program:
hadoop jar totalnumber.jar totalnumber data.txt output(output must be a new directory)
5.See the outcome:
hadoop fs -cat output/*