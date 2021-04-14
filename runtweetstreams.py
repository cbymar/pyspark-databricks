import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
import urllib3
import pandas as pd
from collections import namedtuple
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import desc

sc.stop()
sc = SparkContext().getOrCreate()
ssc = StreamingContext(sc, 5)  # 5 seconds minibatch
sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream("127.0.0.1", 9999)
lines = socket_stream.window(20)
fields = ("tag", "count")
Tweet = namedtuple("Tweet", fields)

schema = StructType([StructField('Tweet', StringType(), True),
                     StructField('tag', StringType(), True),
                     StructField('count', IntegerType(), True)])

lines.flatMap(lambda text: text.split(" ")) \
    .filter(lambda word: word.lower.startswith("#")) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda rec: Tweet(rec[0], rec[1])) \
    .foreachRDD(lambda rdd: sqlContext.createDataFrame(rdd, schema=schema)
                .sort(desc("count"))
                .limit(10).registerTempTable("tweets"))


"""
$ python tweetstreams.py  # in same virtualenv
"""

ssc.start()

count = 0
while count < 10:
    time.sleep(4)
    top_10_tweets = sqlContext.sql('SELECT tag, count FROM tweets')
    top_10_df = top_10_tweets.toPandas()
    display.clear_output(wait=True)
    # if len(top_10_df) > 0:
    plt.figure(figsize=(10, 8))
    sns.barplot(x="count", y="tag", data=top_10_df)
    plt.show()
    count += 1

ssc.stop()
