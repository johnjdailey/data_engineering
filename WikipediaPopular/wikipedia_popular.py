from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'

def time_count(line):
    data = line.split()
    timestamp = data[0]
    project_name = data[1]
    title = data[2]
    count = int(data[3])
    return(timestamp,project_name,title,count)

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

#read the input lines as rdd
text = sc.textFile(inputs)

#split each line to array of strings
split_rdd = text.map(time_count)

# Filter the project_name for "en" and drop any titles with "Main_page" or starting from "Speical:"
filter_rdd = split_rdd.filter(lambda x : x[1] == "en" and not x[2].startswith("Special:") and not x[2] == "Main_Page").map(lambda x : (x[0], (x[3], x[2])))

# Get max count for each key
max_rdd = filter_rdd.reduceByKey(max)

#sort the data and convert to printing format
outdata = max_rdd.sortBy(get_key).map(tab_separated)

#write rdd to a file
outdata.saveAsTextFile(output)



def f(x): return max(x[0])
filter_rdd.mapValues(f)


filter_rdd = split_rdd.filter(lambda x : x[1] == "en" and not x[2].startswith("Special:") and not x[2] == "Main_Page").cache()
a = filter_rdd.map(lambda x : (x[0], x[3]))
max_rdd = a.reduceByKey(max)


[('1', (7, 'b')), ('2', (343, 'tg'))] 



def func(kv1,kv2):
    max=0
    print(kv1)
    print(kv2)
    print("------")
    if kv1 >= kv2
	max = kv1
    return max

    
  
filter_rdd.reduceByKey(func).collect()



