from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)

def check_key(parse_json):
    if 'subreddit' not in parse_json or 'score' not in parse_json or 'author' not in parse_json:
        return(None)
    else:
        return(parse_json['subreddit'],parse_json['score'],parse_json['author'])          

def main(inputs, output):
    in_json = sc.textFile(inputs)
    parse_json = in_json.map(json.loads)
    objs = parse_json.map(check_key).filter(lambda val : val != None)
    filter_data = objs.filter(lambda x : "e" in x[0]).cache() #same rdd is needed to get both positive and negative scores
    positive = filter_data.filter(lambda x : x[1]>0)
    negative = filter_data.filter(lambda x : x[1]<=0)
    positive.map(json.dumps).saveAsTextFile(output + '/positive')
    negative.map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

# reddit-3 with cache()
#real	0m32.660s
#user	0m24.884s
#sys	0m1.324s

# reddit-3 without cache()
#real	0m37.312s
#user	0m23.296s
#sys	0m1.512s

