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
from pyspark.sql.functions import lit
import re

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

#redshift cred
REDSHIFT_HOSTNAME = 'venkat-test.cfxcbauz3avq.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_USERNAME = 'venkattest'
REDSHIFT_PASSWORD = 'BPOahslA9ytWRivguhkV'
REDSHIFT_DATABASE = 'Venkat-test'
REDSHIFT_PAGEVIEW_TBL = 'PageView'

#kinesis cred
APPLICATION_NAME = 'samsanalyticsprocessor'
STREAM_NAME = 'SAMSAnalytics'
REGION_NAME = 'us-east-1'
INITIAL_POS = InitialPositionInStream.LATEST
CHECKPOINT_INTERVAL = 2
ENDPOINT = 'https://kinesis.us-east-1.amazonaws.com'
AWSACCESSID = 'AKIAJSK34E5YQK36DMRQ'
AWSSECRETKEY = '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'

#S3 cred
S3ACCESSID = 'AKIAJCAETS5FY6O5FBJQ'
S3SECRETKEY = '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'
BUCKET = 'sams-staging'
FOLDER = 'wfspoc'
	
def WriteToTable(df, type):
	if type in REDSHIFT_PAGEVIEW_TBL:
		df = df.groupby([COL_STARTTIME, COL_ENDTIME, COL_CUSTOMERID, COL_PROJECTID, COL_FONTTYPE, COL_DOMAINNAME, COL_USERAGENT]).count()
		df = df.withColumnRenamed('count', COL_PAGEVIEWCOUNT)
		
		# Write back to a table
		
		url = ("jdbc:redshift://" + REDSHIFT_HOSTNAME + ":" + REDSHIFT_PORT + "/" +   REDSHIFT_DATABASE + "?user=" + REDSHIFT_USERNAME + "&password="+ REDSHIFT_PASSWORD)
		
		s3Dir = 's3://' + AWSACCESSID + ':' + AWSSECRETKEY + '@' + BUCKET + '/' + FOLDER
		
		print url
		print 's3 url : ', s3Dir
		
		df.write.format("com.databricks.spark.redshift").option("url", url).option("dbtable", REDSHIFT_PAGEVIEW_TBL).option('tempdir', s3Dir).mode('Append').save()

def getSqlContextInstance(sparkContext):
	if ('sqlContextSingletonInstance' not in globals()):
        	globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    	return globals()['sqlContextSingletonInstance']

def getBrowser(userAgent):
	# check for empty or null uri
	print userAgent
	if userAgent:
		browser =  re.search(r'\w+/', userAgent)
		if browser is not None:
			return browser.group(0)[:-1].lower()
		else:
			return None
	else:
		return None

def registerUDF(sqlContext):
	
	#register all user defined functions
	sqlContext.registerFunction("getBrowser", getBrowser)
	sqlContext.registerFunction("setColValues", setColValues)
	

def getCurrentTimeStamp():
	d = datetime.utcnow()
	unixtime = calendar.timegm(d.utctimetuple())
	return unixtime

# cs-uri-stem, c-user-agent
def setColValues(uri, type):
	
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
				if revfont is not None:
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
	registerUDF(sqlContext)

	desiredCol = ['c-ip','cs-uri-stem','c-user-agent','customer-id','x-ec_custom-1']
	if rdd.count() > 0:
		df = pycsv.csvToDataFrame(sqlContext, rdd, columns=columns)
		df = df.select(desiredCol)
	
		#startTime
		endTime = getCurrentTimeStamp()
		startTime = endTime - 10
		df = df.withColumn(COL_STARTTIME, lit(startTime))
		
		#endTime
		df = df.withColumn(COL_ENDTIME, lit(endTime))
		
		df.registerTempTable("tempTable")
		query = ('select' + 
				' startTime,' +  																		#startTime
				' endTime,' +  																			#endTime				
				' `customer-id` as ' +  COL_CUSTOMERID +  ',' +										#customerid				
				' setColValues(`cs-uri-stem`, ' +  '\'' + COL_PROJECTID + '\') as ' +  COL_PROJECTID + ',' +	#projectid					 	
				' setColValues(`cs-uri-stem`, ' +  '\'' + COL_FONTTYPE + '\') as ' +  COL_FONTTYPE +  ',' + 	#FontType
				' setColValues(`cs-uri-stem`, ' +  '\'' + COL_FONTID + '\') as ' +  COL_FONTID +  ',' + 		#FontId
				' `x-ec_custom-1` as ' +  COL_DOMAINNAME +  ',' + 									#DomainName
				' getBrowser(`c-user-agent`) as ' + COL_USERAGENT +  ',' + 							#UserAgent
				' `c-ip` as ' +  COL_IPADDRESS + 														#customer ipaddress   
				' from tempTable')
				
		
		print(query)
		df = sqlContext.sql(query)
		print df.head(10)
		
		WriteToTable(df, REDSHIFT_PAGEVIEW_TBL)
				
if __name__ == "__main__":
	#_conf = new SparkConf(true)
	sc = SparkContext("local[*]", "kinesis")
	ssc = StreamingContext(sc, 10)

	sc.addPyFile('pyspark_csv.py')

	print "Streaming started"	

	kinesisStream = KinesisUtils.createStream(ssc, APPLICATION_NAME, STREAM_NAME, ENDPOINT, REGION_NAME, INITIAL_POS, CHECKPOINT_INTERVAL, awsAccessKeyId =AWSACCESSID, awsSecretKey=AWSSECRETKEY)    
	
	#kinesisStream.reduceByKey(lambda x,y: x+y)
	kinesisStream.count().pprint()

	kinesisStream.foreachRDD(processRdd)
	
	ssc.start()
    	ssc.awaitTermination()
	print "Streaming suspended"


