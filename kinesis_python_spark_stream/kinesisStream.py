from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark_csv as pycsv
from time import gmtime, strftime
from pyspark.sql.functions import unix_timestamp
from urlparse import urlparse
from urlparse import parse_qs
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType
from datetime import datetime
import calendar


columns = ['LogFilename','RowNumber','timestamp','time-taken','c-ip','filesize','s-ip','s-port','sc-status','sc-bytes','cs-method','cs-uri-stem','-','rs-duration','rs-bytes','c-referrer','c-user-agent','customer-id','x-ec_custom-1']

COL_STARTTIME = 'startTime'
COL_ENDTIME = 'endTime'
COL_CUSTOMERID = 'CustomerId'
COL_PROJECTID = 'projectId'
COL_FONTTYPE = 'fontType'
COL_FONTID = 'fontId'
COL_DOMAINNAME = 'domainName'
COL_USERAGENT = 'userAgent'
COL_IPADDRESS = 'ipAddress'
COL_GEOLOCATION = 'geoLocation'
COL_PAGEVIEWCOUNT = 'pageViewCount'

PROJECTID = 'projectId'

REDSHIFT_HOSTNAME = 'venkat-test.cfxcbauz3avq.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_USERNAME = 'venkattest'
REDSHIFT_PASSWORD = 'BPOahslA9ytWRivguhkV'
REDSHIFT_DATABASE = 'Venkat-test'
REDSHIFT_PAGEVIEW_TBL = 'PageView'



def WriteToTable(df, type):
	if type in REDSHIRT_PAGEVIEW_TBL:
		df = df.groupby([COL_STARTTIME, COL_ENDTIME, COL_CUSTOMERID, COL_PROJECTID, COL_FONTTYPE, COL_DOMAINNAME, COL_USERAGENT]).count()
		df = df.withColumnRenamed('count', COL_PAGEVIEWCOUNT)
		
		# Write back to a table
		df.write \
		  .format("com.databricks.spark.redshift") \
		  .option("url", "jdbc:redshift://", REDSHIFT_HOSTNAME, ":", REDSHIFT_PORT, "/", REDSHIFT_DATABASE, "?user=", REDSHIFT_USERNAME, "&password=", REDSHIFT_PASSWORD) \
		  .option("dbtable", SUMMARY_PAGEVIEW_TBL) \		  
		  .mode("error") \
		  .save()

def getSqlContextInstance(sparkContext):
	if ('sqlContextSingletonInstance' not in globals()):
        	globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    	return globals()['sqlContextSingletonInstance']

def getBrowser(userAgent):
	# check for empty or null uri
	if userAgent:
		browser =  re.search(r'\w+/', userAgent)
		if browser not None:
			return browser[-1].lower()
		else:
			return None
	else:
		return None

def registerUDF():
	
	#register all user defined functions
	udf(getBrowser)
	udf(setColValues)
	

def getCurrentTimeStamp():
	d = datetime.utcnow()
	unixtime = calendar.timegm(d.utctimetuple())
	print unixtime

# cs-uri-stem, c-user-agent
def setColValues(x, type):
	
	uri = x.cs-uri-stem
	userAgent = x.c-user-agent
	
	if uri:
		o = urlparse(uri)
		pathParams = parse_qs(o.path)
		queryParams = parse_qs(o.query)
		
		if type in COL_PROJECTID:
			if PROJECTID in queryParams:
				return queryParams[PROJECTID]
		elif type in COL_FONTTYPE or type in COL_FONTID:
			if pathParams:
				revfont = re.search('\w+\.\w+/',o.path[::-1])
				if revfont not None:
					font = revfontType[::-1][1:]
					if font:
						fontSplit = re.split('\.',font)
						if type in COL_FONTID:
							return fontSplit[0]
						elif type in COL_FONTTYPE:
							return fontSplit[1]
		
		return None
	
		
def processRdd(rdd):
	
	#covnert to a dataframe from rdd
	sqlContext = SQLContext(rdd.context)
	registerUDF()

	desirecCol = ['c-ip','cs-uri-stem','c-user-agent','customer-id','x-ec_custom-1']
	if rdd.count() > 0:
		df = pycsv.csvToDataFrame(sqlContext, rdd, columns=columns)
		df = df.select(desiredCol)
	
		#startTime
		endtime = getCurrentTimeStamp()
		startTime = endtime - 10
		df = df.withColumn(COL_STARTTIME, startTime)
		
		#endTime
		df = df.withColumn(COL_ENDTIME, endTime)
		
		df.registerTempTable("tempTable")
		query = 'select',
				'startTime,', 																#startTime
				'endTime,', 																#endTime
				'customer-id as ', COL_CUSTOMERID, ',',										#customerid
				'setColValues(cs-uri-stem, ', COL_PROJECTID,') as ', COL_PROJECTID, ',',	#projectid
				'setColValues(cs-uri-stem, ', COL_FONTTYPE,') as ', COL_FONTTYPE, ',',		#FontType
				'setColValues(cs-uri-stem, ', COL_FONTID,') as ', COL_FONTID, ',',			#FontId
				'x-ec_custom-1 as ', COL_DOMAINNAME, ',',									#DomainName
				'getBrowser(c-user-agent) as ', COL_USERAGENT, ',',							#UserAgent
				'c-ip as ', COL_IPADDRESS													#customer ipaddress
		
		print(query)
		df = sqlContext.sql(query)
		
		WriteToTable(df, REDSHIFT_PAGEVIEW_TBL)
				
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


