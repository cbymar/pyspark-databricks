from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
SparkContext (vs spark session)
-- StreamingContext
---- SocketTextStream
Split input line, map to a tuple, reduce by key
Note that RDD syntax makes frequent use of lambda functions
Terminal is open 
"""

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)  # context, interval in seconds

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
word_counts = pairs.reduceByKey(lambda num1, num2: num1 + num2)
word_counts.pprint()  # just pretty printing them
ssc.start()

"""
on terminal
$ nc -lk 9999

"""
word_counts.pprint()

ssc.stop()

"""
More streaming notes:
https://www.youtube.com/watch?v=lzHC8NQD3jQ
https://jeremykun.com/2015/03/09/finding-the-majority-element-of-a-stream/
https://github.com/LaurentRDC/npstreams
https://hackmag.com/coding/lets-tame-data-streams-with-python/
https://www.svds.com/streaming-video-analysis-python/
"""
