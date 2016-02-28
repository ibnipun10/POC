from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark_csv as pycsv
from time import gmtime, strftime

columns = ['LogFilename','RowNumber','timestamp','time-taken','c-ip','filesize','s-ip','s-port','sc-status','sc-bytes','cs-method','cs-uri-stem','-','rs-duration','rs-bytes','c-referrer','c-user-agent','customer-id','x-ec_custom-1']

def getSqlContextInstance(sparkContext):
	if ('sqlContextSingletonInstance' not in globals()):
        	globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    	return globals()['sqlContextSingletonInstance']


def processRdd(rdd):
	
	#covnert to a dataframe from rdd
	sqlContext = SQLContext(rdd.context)

	desirecCol = ['c-ip','cs-uri-stem','c-user-agent','customer-id','x-ec_custom-1']
	if rdd.count() > 0:
		df = pycsv.csvToDataFrame(sqlContext, rdd, columns=columns)
		df = df.select(desiredCol)

		
		endTime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		df.registerTempTable("tempTable")
		
			
	

if __name__ == "__main__":
	#_conf = new SparkConf(true)
	sc = SparkContext("local[*]", "kinesis")
	ssc = StreamingContext(sc, 10)

	sc.addPyFile('pyspark_csv.py')

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
	
	#kinesisStream.reduceByKey(lambda x,y: x+y)
	kinesisStream.count().pprint()

	kinesisStream.foreachRDD(processRdd)
	
	ssc.start()
    	ssc.awaitTermination()
	print "Streaming suspended"


