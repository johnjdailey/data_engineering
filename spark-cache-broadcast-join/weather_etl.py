import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Weather ETL').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+    

def main(inputs, output):
	observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False)])

	weather = spark.read.csv(inputs, schema=observation_schema)

	filter_df = weather.filter((weather.qflag.isNull()) & (weather.station.startswith('CA')) & (weather.observation == 'TMAX'))

	temp_df = filter_df.withColumn('tmax',filter_df.value/10)

	clean_df = temp_df.select('station', 'date', 'tmax')

	clean_df.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
