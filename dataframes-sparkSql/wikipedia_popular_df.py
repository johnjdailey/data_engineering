import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName('Wikipedia Popular').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+ 

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    arr = path.split('/')
    last_str = arr[-1]
    result = re.search('pagecounts-(.*)0000', last_str)
    return result.group(1)

def main(inputs, output):
	log_schema = types.StructType([
    types.StructField('project', types.StringType(), False),
    types.StructField('title', types.StringType(), False),
    types.StructField('views', types.IntegerType(), False),
    types.StructField('size', types.IntegerType(), False)
    ])

	in_df = spark.read.csv(inputs, schema=log_schema, sep=" ").withColumn('filename', functions.input_file_name())

	hour_df = in_df.withColumn("hour", path_to_hour(in_df.filename)).drop(in_df.filename)

	get_en = hour_df.filter(hour_df.project == "en")

	fil_1 = get_en.filter(get_en.title != "Main_Page")

	fil_2 = fil_1.filter(~fil_1.title.startswith("Special:")).cache()

	hour_max = fil_2.groupBy(fil_2.hour).agg(functions.max(fil_2['views']).alias('max_count'))

	count_join = fil_2.join(broadcast(hour_max), (hour_max.max_count == fil_2.views) & (hour_max.hour == fil_2.hour)).select(fil_2.hour, fil_2.title, fil_2.views)

	res_sort = count_join.sort(count_join.hour, count_join.title)

	res_sort.write.json(output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)