import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName('temp range').getOrCreate()
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

    filter_df = weather.filter(weather.qflag.isNull()).cache()
    #filter_df1 = filter_df.filter((filter_df.observation == 'TMAX') | (filter_df.observation == 'TMIN'))

    df_tmax = filter_df.filter(filter_df.observation == 'TMAX').selectExpr('station as tmax_station', 'date as tmax_date', 'observation as tmax', 'value as tmax_value')
    
    df_tmin = filter_df.filter(filter_df.observation == 'TMIN').selectExpr('station as tmin_station', 'date as tmin_date', 'observation as tmin', 'value as tmin_value')

    get_min_max = df_tmin.join(df_tmax, (df_tmax.tmax_station == df_tmin.tmin_station) & (df_tmax.tmax_date == df_tmin.tmin_date))
    
    get_column = get_min_max.selectExpr('tmax_station as station', 'tmax_date as date', 'tmax_value', 'tmin_value')

    get_range = get_column.withColumn('range', (get_column.tmax_value - get_column.tmin_value)/10).select('date', 'station', 'range').cache()

    max_range_df = get_range.groupBy(get_range.date.alias('max_date')).agg(functions.max(get_range['range']).alias('max_range'))

    max_station = get_range.join(broadcast(max_range_df), (max_range_df.max_date == get_range.date) & (max_range_df.max_range == get_range.range)).select('date','station','range')

    sort_df = max_station.sort(max_station.date, max_station.station)

    sort_df.write.csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
