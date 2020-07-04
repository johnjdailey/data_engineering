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
### 2) AverageJSON-MapReduce
Calculate averages in JSON files using Hadoop MapReduce.
