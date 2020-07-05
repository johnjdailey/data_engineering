import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Reddit Average').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+    

def main(inputs, output):
    comments_schema = types.StructType([
    types.StructField('score', types.LongType(), True),
    types.StructField('subreddit', types.StringType(), True)
    ])

    in_df = spark.read.json(inputs, schema=comments_schema)

    averages = in_df.groupby('subreddit').agg(functions.avg(in_df['score']))
    
    averages.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)