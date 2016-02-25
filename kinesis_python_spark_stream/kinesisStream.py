from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
	#_conf = new SparkConf(true)
	sc = SparkContext("local[*]", "kinesis")
	ssc = StreamingContext(sc, 10)
	print "Streaming started"
	
	applicationName = 'samsanalyticsprocessor'
	streamName = 'SAMSAnalytics'
	regionName = 'us-east-1'
	initialPos = InitialPositionInStream.LATEST
	checkPointInterval = 2
	endpoint = 'https://kinesis.us-east-1.amazonaws.com'
	awsAccessId = 'AKIAJSK34E5YQK36DMRQ'
	awsSecretKey = '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'
	
	kinesisStream = KinesisUtils.createStream(ssc, applicationName, streamName, endpoint, regionName, initialPos, checkPointInterval, awsAccessKeyId =awsAccessId, awsSecretKey=awsSecretKey)
    #streamingContext, [Kinesis app name], [Kinesis stream name], [endpoint URL],[region name], [initial position], [checkpoint interval], StorageLevel.MEMORY_AND_DISK_2)
	
#	print "Count of rdd is : " , kafkaStream.count()
	kinesisStream.pprint()
	
	ssc.start()
    	ssc.awaitTermination()
	print "Streaming suspended"


