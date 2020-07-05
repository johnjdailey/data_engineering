from pyspark import SparkConf, SparkContext
import sys
import random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_random_count(v):
	random.seed()
	avg = 0.0
	for i in range(1,v):
		sums = 0.0
		count = 0
		while sums < 1:
			sums += random.uniform(0,1)
			count += 1
		avg += count/v
	return(avg)

def main(samples):
	samples = int(samples)
	step = 1000
	tracker = sc.range(0, samples, step, numSlices=40).cache()
	sample_count = tracker.map(lambda x: (x,step)).map(lambda x: get_random_count(x[1]))
	euler = sample_count.reduce(operator.add)
	print(euler/tracker.count())


if __name__ == '__main__':
    conf = SparkConf().setAppName('eulers constant')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    samples = sys.argv[1]
    main(samples)

#time-
#real	0m3.874s
#user	0m8.644s
#sys	0m0.884s


