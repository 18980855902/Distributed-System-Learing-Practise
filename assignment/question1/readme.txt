1.Draw the data file.txt and ReplaceTextInFileMapReduce.jar into src directory.
2.Copy dile from NFS to HDFS:
hadoop fs -copyFromLocal vaccination-rates-over-time-by-age.txt ~
3.Set the directory that contains the totalnumber.jar file: 
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/comp/$USER/src/* 
4.Run the program:
hadoop jar ReplaceTextInFileMapReduce.jar ReplaceTextInFileMapReduce vaccination-rates-over-time-by-age.txt output(output must be a new directory)
5.See the outcome:
hadoop fs -cat output/*