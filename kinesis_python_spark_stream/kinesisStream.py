from pyspark.streaming.kinesis import KinesisUtils
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
from pyspark.sql.types import NullType
import re
import os
from time import gmtime, strftime
from user_agents import parse
from constants import *
import sys
import traceback

SPARK_APPNAME = 'Kinesis'
SPARK_STREAM_BATCH = 10

def RemoveNone(df):
	return df.fillna('')

def writeToTable(table, groupDf):

	print 'writeToTable'
	
	groupDf = groupDf.withColumnRenamed('count', COL_PAGEVIEWCOUNT)
	
	groupDf = groupDf.coalesce(NUM_PARTITIONS)
	count = groupDf.count()	
	
	printOnConsole(' count of groupdf ' + table + ' is : ' + str(count))
	
	# Write back to a table		
	printOnConsole('Start writing to redshift table : ' + table)
	
	#because of this issue, removing None
	#https://github.com/databricks/spark-redshift/issues/190
	groupDf = RemoveNone(groupDf)
	
	groupDf.write.format("com.databricks.spark.redshift").option("url", REDSHIFT_URL).option("dbtable", table).option('tempdir', S3_URL).mode('Append').save()
	
	printOnConsole('Finished writing to redshift table : ' + table)
	
	
def processForTable(df, type):
	
	print 'processForTable'	
	df.cache()
	
	if PAGEVIEW_TYPE & type:
		groupDf = df.groupby([COL_STARTTIME, COL_ENDTIME, COL_CUSTOMERID, COL_PROJECTID, COL_FONTTYPE, COL_DOMAINNAME, COL_USERAGENT]).count()
		writeToTable(REDSHIFT_PAGEVIEW_TBL, groupDf)
	
	if PAGEVIEWGEO_TYPE & type:
		groupDf = df.groupby([COL_STARTTIME, COL_ENDTIME, COL_CUSTOMERID, COL_PROJECTID, COL_FONTTYPE, COL_FONTID, COL_DOMAINNAME, COL_USERAGENT, COL_IPADDRESS]).count()		
		writeToTable(REDSHIFT_PAGEVIEWGEO_TBL, groupDf)
	
        df.unpersist(False)	
		
def getSqlContextInstance(sparkContext):
	if ('sqlContextSingletonInstance' not in globals()):
        	globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    	return globals()['sqlContextSingletonInstance']

def printOnConsole(line):
	print strftime("%Y-%m-%d %H:%M:%S", gmtime()), '    ', line

def getBrowser(userAgent):
	# check for empty or null uri
	if userAgent:
		user_agent =  parse(userAgent)
		return user_agent.browser.family.lower()
	else:
		return None

def setProjectId(projectid):
	if projectid:
		return projectid
	else:
		return None

def setIpaddress(ipaddress):
	if ipaddress:
		return ipaddress
	else:
		return None

def registerUDF(sqlContext):
	
	#register all user defined functions
	sqlContext.registerFunction("getBrowser", getBrowser)
	sqlContext.registerFunction("setColValues", setColValues)
	sqlContext.registerFunction("getDomainName", getDomainName)
	sqlContext.registerFunction("setProjectId", setProjectId)	
	sqlContext.registerFunction("setIpaddress", setIpaddress)

def getCurrentTimeStamp():
	d = datetime.utcnow()
	unixtime = calendar.timegm(d.utctimetuple())
	return unixtime

def getDateTimeFormat(timestamp):
	return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

# cs-uri-stem, c-user-agent
def setColValues(uri, type):
	
	if uri:
		o = urlparse(uri)
		pathParams = o.path
		
		if o.query is not None:
			# the log is not proper so ading extra logic, multiple projectsids found
			param = o.query.split(')%20format',1)[0]
			queryParams = parse_qs(param)
		
		if type in COL_PROJECTID:
			if PROJECTID in queryParams:
				return queryParams[PROJECTID][0]
		elif type in COL_FONTTYPE or type in COL_FONTID:
			if pathParams:
				revfont = re.search(r'(.*\/)(.*)',pathParams)
				if revfont is not None and revfont.group(2) is not None:
					font = revfont.group(2)
					if font:
						filename, file_extension = os.path.splitext(font)
						if type in COL_FONTID:
							if filename is not None and filename:
								return filename
							else:
								return None
						elif type in COL_FONTTYPE:
							if file_extension is not None and file_extension:
								fontType = file_extension[1:].lower()
								if fontType in FONT_LIST:
									return fontType							 
							else:
								return None
		
	return None
	
def getDomainName(uri):
	
	if uri:
		params = urlparse(uri)
		path = params.hostname
		
		return path
	else:
		return None
	
		
def processRdd(rdd):
	
	try:
		print 'processRDD'
		#covnert to a dataframe from rdd
		
		printOnConsole('Started Processing the streams')

		#desiredCol = ['c-ip','cs-uri-stem','c-user-agent','customer-id','x-ec_custom-1']
		if rdd.count() > 0:
			df = pycsv.csvToDataFrame(sqlContext, rdd, columns=COLUMNS, colTypes=COLUMN_TYPES)
			#df = df.select(desiredCol)
			
			#startTime
			endTime = getCurrentTimeStamp()
			startTime = endTime - SPARK_STREAM_BATCH
			
			endTime = getDateTimeFormat(endTime)
			startTime = getDateTimeFormat(startTime)
			df = df.withColumn(COL_STARTTIME, lit(startTime))
			
			#endTime
			df = df.withColumn(COL_ENDTIME, lit(endTime))

			df.registerTempTable("tempTable")
			query = ('select' + 
					' startTime,' +  																				#startTime
					' endTime,' +  																					#endTime				
					' \'\' as ' +  COL_CUSTOMERID +  ',' +															#customerid				
					' setProjectId(`projectid`) as ' +  COL_PROJECTID + ',' +														#projectid					 	
					' \'\' as ' +  COL_FONTTYPE +  ',' + 															#FontType
					' \'\' as ' +  COL_FONTID +  ',' + 																#FontId
					' getDomainName(`referrer`) as ' +  COL_DOMAINNAME +  ',' + 											#DomainName
					' getBrowser(`useragent`) as ' + COL_USERAGENT +  ',' + 										#UserAgent
					' setIpaddress(`ip`) as ' +  COL_IPADDRESS + 																	#customer ipaddress   
					' from tempTable')

			df = sqlContext.sql(query)
			
			type =  PAGEVIEW_TYPE | PAGEVIEWGEO_TYPE
			processForTable(df, type)
		else:
			printOnConsole('Nothing to process')
	
	except Exception, ex:
		printOnConsole('There was an error...')
		print ex			
	
if __name__ == "__main__":
	#_conf = new SparkConf(true)
	sc = SparkContext(appName = SPARK_APPNAME)
	ssc = StreamingContext(sc, SPARK_STREAM_BATCH)

	sc.addPyFile(CODE_PATH + '/pyspark_csv.py')
        sc.addPyFile(CODE_PATH + '/constants.py')

        sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", S3ACCESSID)
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", S3SECRETKEY)

	sqlContext = SQLContext(sc)
	registerUDF(sqlContext)

	printOnConsole('Streaming started')

	
	kinesisStream = [KinesisUtils.createStream(ssc, APPLICATION_NAME, STREAM_NAME, ENDPOINT, REGION_NAME, INITIAL_POS, CHECKPOINT_INTERVAL, awsAccessKeyId =AWSACCESSID, awsSecretKey=AWSSECRETKEY, storageLevel=STORAGE_LEVEL) for _ in range (NUM_STREAMS)]
	
	unifiedStream = ssc.union(*kinesisStream)
		
	print 'Started running'
	#kinesisStream.reduceByKey(lambda x,y: x+y)
	unifiedStream.count().pprint()

	unifiedStream.foreachRDD(processRdd)
	
	ssc.start()
	ssc.awaitTermination()
	printOnConsole('Streaming suspended')


