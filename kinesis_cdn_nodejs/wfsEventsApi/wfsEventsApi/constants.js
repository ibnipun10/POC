const HOST                      = '0.0.0.0';
const PORT                      = 3000;

// Kinesis
const KINESIS_STREAM            = 'SAMSAnalytics';
const KINESIS_REGION            = 'us-east-1';
const KINESIS_ACCESSID          = 'AKIAJSK34E5YQK36DMRQ';
const KINESIS_SECRET_KEY        = '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';

const RANDOM_LIMIT              = 100000;
const RECORDS_BATCH             = 1;

// Cache
const KEY_RECORDS_COUNT = "COUNT";
const KEY_RECORDS = "RECORDS";

exports.HOST = HOST;
exports.PORT = PORT;
exports.KINESIS_STREAM = KINESIS_STREAM;
exports.KINESIS_REGION = KINESIS_REGION;
exports.KINESIS_ACCESSID = KINESIS_ACCESSID;
exports.KINESIS_SECRET_KEY = KINESIS_SECRET_KEY;
exports.RANDOM_LIMIT = RANDOM_LIMIT;
exports.KEY_RECORDS_COUNT = KEY_RECORDS_COUNT;
exports.KEY_RECORDS = KEY_RECORDS;
exports.RECORDS_BATCH = RECORDS_BATCH;
