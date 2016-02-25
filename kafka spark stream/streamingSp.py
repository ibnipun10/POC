from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
	#_conf = new SparkConf(true)
	sc = SparkContext("local[*]", "count")
	ssc = StreamingContext(sc, 10)
	print "Streaming started"

	kafkaStream = KafkaUtils.createStream(ssc, 'noi-sams-spark-w3:2181', "spark-streaming-consumer", topics={'samstest':1})
	
#	print "Count of rdd is : " , kafkaStream.count()
	kafkaStream.pprint()
	ssc.start()
    	ssc.awaitTermination()
	print "Streaming suspended"
