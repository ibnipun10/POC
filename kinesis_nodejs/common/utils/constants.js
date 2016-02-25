var utils = require('./util.js');

const EVENTS_APIKEY_TBL           = 'apikey',
      EVENTS_EVENTNAME_TBL        = 'eventname',
      EVENTS_EVENTS_TBL           = 'events',
      EVENTS_EVENTTRANSACTION_TBL = 'eventtransactions',
      EVENTS_BRANDNAME_TBL        = 'brandname',
      EVENTS_ERRORLOGS_TBL        = 'errorlogs',
      EVENTS_KEYSPACE             = 'sams';
     

const CASSANDRA_SERVER          = ['172.28.0.162'];
const KINESIS_STREAM           = 'SAMSAnalytics';
const KINESIS_REGION           = 'us-east-1';

const ENABLE_HTTPS              = false;
const ENABLE_ERRORLOGS          = false;

const IS_PRODUCTION = true;

const VALIDATION_TYPE_SERVER            = 1;
const VALIDATION_TYPE_META              = 2;
const VALIDATION_TYPE_REQUIREDFIELDS    = 3;
const NULL_REQUIRED_FIELDS = 4;

const TTL_VALUE = 3600;
const CACHEMETA = 1;
const CACHEREQUIRED = 2;
const MYCACHE = 3;
const CACHETTL = 4;
const DEFAULT_PERPAGE = 25;

const SERVER_HOSTNAME = utils.getHostName();

const RANDOM_LOWER_LIMIT = 0;
const RANDOM_HIGHER_LIMIT = 10;

const SERIALS = "serials";
const SERIALS_BRAND = "brand";
const SERIALS_PACK = "pack";
const SERIALS_ASSET = "asset";
const SERIALS_DELIMITER = '_';


//Error Constants

const ERROR_APPNAME = "Invalid API key or app Name",
      ERROR_EVENTS_API = "The Swyft Events API is down.  We are investigating",
      ERROR_META_DATA = "Invalid meta data received. Check API documentation for valid meta parameters",
      ERROR_REQUIRED_FIELDS = "Invalid required fields received. Check API documentation for valid serial parameters",
      ERROR_EVENT_TIME = "Invalid event time",
      ERROR_META_OR_REQUIRED = "Invalid meta or required fields",
      ERROR_EVENT_NAME = "Invalid event name",
      ERROR_SERIALS = "Invalid serials"
      ERROR_INVALID_DATA = "Invalid data",
      ERROR_BAD_REQUEST = "Bad Request",
      ERROR_NO_DATA = "No data",
      ERROR_INVALID_KEY = "Invalid key";
      ERROR_EVENTALREADY_PRESENT = "EventName already present",
      ERROR_APPNAME_ALREADY_PRESENT = "AppName already present";
      ERROR_APPNAME_NOT_PRESENT = "AppName not present",
      ERROR_NULL_REQUIRED_FIELDS = "Required fields cannot be null";


const AUTHORIZING_KEY = "8ac49802f4c38861ef86bbfc";
const HEADER_AUTHORIZATION = "Authorization";

//Exporting table names
exports.EVENTS_APIKEY_TBL = EVENTS_APIKEY_TBL;
exports.EVENTS_EVENTNAME_TBL = EVENTS_EVENTNAME_TBL;
exports.EVENTS_EVENTS_TBL = EVENTS_EVENTS_TBL;
exports.EVENTS_EVENTTRANSACTION_TBL = EVENTS_EVENTTRANSACTION_TBL;
exports.EVENTS_BRANDNAME_TBL = EVENTS_BRANDNAME_TBL;
exports.EVENTS_ERRORLOGS_TBL = EVENTS_ERRORLOGS_TBL;
exports.EVENTS_KEYSPACE = EVENTS_KEYSPACE;

//exporting cassandra server
exports.CASSANDRA_SERVER = CASSANDRA_SERVER;


//exporting toggles
exports.ENABLE_HTTPS = ENABLE_HTTPS;
exports.ENABLE_ERRORLOGS = ENABLE_ERRORLOGS;

exports.VALIDATION_TYPE_SERVER = VALIDATION_TYPE_SERVER;
exports.VALIDATION_TYPE_META = VALIDATION_TYPE_META;
exports.VALIDATION_TYPE_REQUIREDFIELDS = VALIDATION_TYPE_REQUIREDFIELDS;
exports.NULL_REQUIRED_FIELDS = NULL_REQUIRED_FIELDS;

exports.SERVER_HOSTNAME = SERVER_HOSTNAME;


exports.TTL_VALUE = TTL_VALUE;
exports.CACHEMETA = CACHEMETA;
exports.CACHEREQUIRED = CACHEREQUIRED;
exports.MYCACHE = MYCACHE;
exports.IS_PRODUCTION = IS_PRODUCTION;


//Exporting error constants
exports.ERROR_APPNAME = ERROR_APPNAME;
exports.ERROR_EVENTS_API = ERROR_EVENTS_API;
exports.ERROR_META_DATA = ERROR_META_DATA;
exports.ERROR_REQUIRED_FIELDS = ERROR_REQUIRED_FIELDS;
exports.ERROR_EVENT_TIME = ERROR_EVENT_TIME;
exports.ERROR_META_OR_REQUIRED = ERROR_META_OR_REQUIRED;
exports.ERROR_EVENT_NAME = ERROR_EVENT_NAME;
exports.ERROR_INVALID_DATA = ERROR_INVALID_DATA;
exports.ERROR_BAD_REQUEST = ERROR_BAD_REQUEST;
exports.ERROR_NO_DATA = ERROR_NO_DATA;
exports.AUTHORIZING_KEY = AUTHORIZING_KEY;
exports.HEADER_AUTHORIZATION = HEADER_AUTHORIZATION;
exports.ERROR_INVALID_KEY = ERROR_INVALID_KEY;
exports.ERROR_EVENTALREADY_PRESENT = ERROR_EVENTALREADY_PRESENT;
exports.ERROR_APPNAME_ALREADY_PRESENT = ERROR_APPNAME_ALREADY_PRESENT;
exports.ERROR_APPNAME_NOT_PRESENT = ERROR_APPNAME_NOT_PRESENT;
exports.ERROR_NULL_REQUIRED_FIELDS = ERROR_NULL_REQUIRED_FIELDS;
exports.ERROR_SERIALS = ERROR_SERIALS;
exports.DEFAULT_PERPAGE = DEFAULT_PERPAGE;

exports.RANDOM_LOWER_LIMIT = RANDOM_LOWER_LIMIT;
exports.RANDOM_HIGHER_LIMIT = RANDOM_HIGHER_LIMIT;

exports.SERIALS = SERIALS;
exports.SERIALS_BRAND = SERIALS_BRAND;
exports.SERIALS_PACK = SERIALS_PACK;
exports.SERIALS_ASSET = SERIALS_ASSET;
exports.SERIALS_DELIMITER = SERIALS_DELIMITER;

exports.CACHETTL = CACHETTL;
exports.KINESIS_STREAM = KINESIS_STREAM;
exports.KINESIS_REGION = KINESIS_REGION;
