const HOST                      = '0.0.0.0';
const PORT                      = 3000;
const KINESIS_STREAM            = 'SAMSAnalytics';
const KINESIS_REGION            = 'us-east-1';
const RANDOM_LIMIT              = 100000;
const RECORDS_BATCH             = 1;

// Cache
const KEY_RECORDS_COUNT = "COUNT";
const KEY_RECORDS = "RECORDS";

exports.HOST = HOST;
exports.PORT = PORT;
exports.KINESIS_STREAM = KINESIS_STREAM;
exports.KINESIS_REGION = KINESIS_REGION;
exports.RANDOM_LIMIT = RANDOM_LIMIT;
exports.KEY_RECORDS_COUNT = KEY_RECORDS_COUNT;
exports.KEY_RECORDS = KEY_RECORDS;
exports.RECORDS_BATCH = RECORDS_BATCH;
