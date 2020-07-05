import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName('temp range sql').getOrCreate()
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
    weather.createOrReplaceTempView('weather')

    filters = spark.sql(
    	"""select station, date, observation, value 
		from weather 
		where qflag is null and (observation == \"TMIN\" or observation == \"TMAX\")"""
    	).cache()
    filters.createOrReplaceTempView('filters')

    tmax = spark.sql(
		"""select station, date, observation, value 
		from filters 
		where observation == \"TMAX\""""
    	)
    tmax.createOrReplaceTempView("tmax")

    tmin = spark.sql(
    	"""select station, date, observation, value 
    	from filters 
    	where observation == \"TMIN\""""
    	)
    tmin.createOrReplaceTempView("tmin")

    get_min_max = spark.sql(
    	"""select tmax.station, tmax.date, tmax.value as tmax, tmin.value as tmin 
    	from tmax 
    	join tmin 
    	on (tmax.station = tmin.station and tmax.date = tmin.date)"""
    	)
    get_min_max.createOrReplaceTempView('get_min_max')

    get_range = spark.sql(
    	"""select station, date, ((tmax-tmin)/10) as range 
    	from get_min_max"""
    	).cache()
    get_range.createOrReplaceTempView('get_range')

    max_range = spark.sql(
    	"""select date, max(range) as max_range 
    	from get_range 
    	group by date"""
    	)
    max_range.createOrReplaceTempView("max_range")

    max_station = spark.sql(
    	"""select get_range.date, station, range 
    	from get_range 
    	join max_range 
    	on (max_range.date == get_range.date and max_range.max_range = get_range.range) 
    	order by get_range.date, station"""
    	)
    max_station.write.csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
