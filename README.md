# data_engineering
The repository contains multiple examples of perfoeming ETL using Spark and Hadoop MR.

### 1) WordCount-MapReduce

Count number of words in text files stored in HDFS.
- `hdfs dfs` can be used to copy files from disk to HDFS.
- The Mapper and Reducer classes in java perform MapReduce operations.
- `yarn` is used to submit the MR job to hadoop cluster.
- The number of reducers can be provided using `mapreduce.job.reduces` (default=1). Having multiple reducers with larger output allows the process to be more efficient as the reducing task is split between the nodes and performed parallely.
```sh 
hdfs -mkdir {dir}
hdfs df -copyFromLocal {source} {dir}
hdfs -ls

yarn jar wordcount.jar WordCount -D mapreduce.job.reduces=n {input} {output}
```
### 2) WordCountImproved-MapReduce
Slight variation in WordCount-MapReduce to improve efficiency. 

### 3) AverageRedditScore-MapReduce
Calculate averages scores from subreddits avaiable in Reddit Comments Corpus as JSON files using Hadoop MapReduce.

### 4) WikipediaPopular
Used page view statistics published by wikipedia to calculate number of times a page was visited in an hour. 
- The code was implemented both using Mapper/Reducer in java and Spark in Python.
- PySpark is run using `spark-submit`
```sh
spark-submit wikipedia_popular.py {input} {output}
```

### 5) WordCount-Pyspark
The word count implementation using PySpark.

### 6) WordCountImproved-PySpark
Slight variation in WordCount-Pyspark to improve efficiency.

### 7) euler_constant 
Calculate euler constant using PySpark.

### 8) spark-cache-broadcast-join
This set of examples goes deeper into operations performed by spark, this includes- joins, cache and broadcast.
- `.cache()`: Spark uses lazy evaluation and intermediate results are discarded. In case, the code needs to reuse a variable it will get re-computed. This can be overcome by caching the intermediate variable using .cache()
- `.join()`:  It is used to join two RDDs or Dataframes. If we are working in a cluster with multiple executers the the join operation will cause a lot of shuffling. This causes delays and uses a lot more memory. Solution -> broadcast.
- `.broadcast()`: Broadcasting an RDD will copy the entire RDD to each executer which will avoid the excess shuffling of data.


