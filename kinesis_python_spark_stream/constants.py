from pyspark.streaming.kinesis import InitialPositionInStream
from pyspark.storagelevel import StorageLevel

NUM_STREAMS = 1
NUM_EXECUTORS = 2
NUM_CORES = 2
NUM_PARTITIONS = 3 * NUM_CORES * NUM_EXECUTORS

HOME_PATH = '/home/hadoop'
FILE_PATH = '/POC/kinesis_python_spark_stream'
CODE_PATH = HOME_PATH + FILE_PATH

COLUMNS = ['uri', 'projectid', 'ip', 'timestamp', 'useragent', 'referrer', 'httpstatus']
COLUMN_TYPES = ['string', 'string', 'string', 'string', 'string', 'string', 'string']

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
REDSHIFT_HOSTNAME = 'sams-poc.cfxcbauz3avq.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_USERNAME = 'venkattest'
REDSHIFT_PASSWORD = 'BPOahslA9ytWRivguhkV'
REDSHIFT_DATABASE = 'venkattest'
REDSHIFT_PAGEVIEW_TBL = 'PageView'
REDSHIFT_PAGEVIEWGEO_TBL = 'PageViewGeo'
REDSHIFT_URL = ("jdbc:redshift://" + REDSHIFT_HOSTNAME + ":" + REDSHIFT_PORT + "/" +   REDSHIFT_DATABASE + "?user=" + REDSHIFT_USERNAME + "&password="+ REDSHIFT_PASSWORD)

#kinesis cred
APPLICATION_NAME = 'samsanalyticsprocessor'
STREAM_NAME = 'SAMSAnalytics'
REGION_NAME = 'us-east-1'
INITIAL_POS = InitialPositionInStream.LATEST
CHECKPOINT_INTERVAL = 60
ENDPOINT = 'https://kinesis.us-east-1.amazonaws.com'
AWSACCESSID = 'AKIAJSK34E5YQK36DMRQ'
AWSSECRETKEY = '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'
STORAGE_LEVEL = StorageLevel.MEMORY_ONLY

#S3 cred
S3ACCESSID = 'AKIAJSK34E5YQK36DMRQ'
S3SECRETKEY = '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'
BUCKET = 'sams-analytics-poc'
FOLDER = 'sams-poc'
S3_URL = 's3n://' + BUCKET + '/' + FOLDER

FONT_LIST = ['ttf', 'woff', 'woff2', 'svg', 'eot', 'ttf-1', 'woff-3', 'woff2-14', 'svg-11', 'eot-2']

#table type
PAGEVIEW_TYPE = 1
PAGEVIEWGEO_TYPE = 2
