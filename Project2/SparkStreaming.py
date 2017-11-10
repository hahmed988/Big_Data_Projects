from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 0.5)
def toCSVLine(data):
    return ','.join(str(d) for d in data)
COUNT = 0
def checkRddAndWrite(rdd):
        if not rdd.isEmpty():
                global COUNT
                rdd = rdd.map(lambda line : line.split(",")).filter(lambda line : "A" in line[4])
                rdd = rdd.map(lambda line : (line[0],line[1],line[2],line[3]))
                rdd = rdd.map(toCSVLine)
                path = "/user/1289B29/uber/StreamData/" + str(COUNT)
                rdd.coalesce(1).saveAsTextFile(path)
                COUNT = COUNT + 1
lines = ssc.socketTextStream("172.16.0.238", 9994)
lines.foreachRDD(lambda rdd : checkRddAndWrite(rdd))

ssc.start()             # Start the computation
ssc.awaitTermination()