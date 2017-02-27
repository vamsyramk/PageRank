Difference between Spark and MapReduce:
1. Spark is much quicker compared to Map Reduce.
2. Temporary files are not getting created for the Spark.
3. The implementation of pagerank is much easier in Spark than in Mapreduce Due to built in functions.
4. In spark we can't modify the RDD but we can only transform the RDDs as it is based on coarse 
grained implementation.
5. MapReduce provides easier way to modify the files as it is not a coarse grained implementation.
6. Spark provides a way to store the RDDs in the cache.
7. Spark is interactive and data flow can be traced easily.
8. In mapreduce we have to pass the data between various mappers reducers.
9. Debugging is much easier in spark compared to mapreduce.
10.The transformations does almost most of the job required for the job
11.The calculation in Spark is lazy, means transformations does not get applied untill an action is called on it.
12.Spark performs in-memory processing of data which makes spark faster than mapreduce.
13.MapReduce is written in Java, where as spark is mostly written in python or scala
14.MapReduce performs batch processing where as spark provides real-time processing.

MapReduce Implementation:
1.Command to run map reduce program is 
	Compilation:
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint
	execution:
	hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/input /user/cloudera/output0
2.Please add a 0(zero) to the name of the output foler as above, and the final output will be in "output11"
after 10 iterations and after the final sorting step.


Spark:
1.Execution command for spark is "spark-submit pagerank.py inputdirectory outputdirectory"
Example: spark-submit pagerank.py input output
