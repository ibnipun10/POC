var AWS = require('aws-sdk'),
    constants = require('./constants'),
    utils = require('./utils.js'),
    cache = require('./cacheHelper.js');

var kinesis = new AWS.Kinesis({ region : constants.KINESIS_REGION });

module.exports = {
    
    createRecord: function (data){
        return {
            Data: data, 
            PartitionKey: 'pk-' + utils.randomNum()
        }
    },

    writeToKinesis: function (data) {
        
        recCount = cache.getKey(constants.KEY_RECORDS_COUNT);
        var records = cache.getKey(constants.KEY_RECORDS);
        
        if (recCount == null || recCount < constants.RECORDS_BATCH) {
            
            if (records == null) {
                records = [];
                recCount = 0;
            }
        }
        else {
            //write to kinesis
            var recordParams = {
                Records: records,                
                StreamName: constants.KINESIS_STREAM
            };
            
            records = [];
            recCount = 0;

            kinesis.putRecords(recordParams, function (err, data) {
                if (err) console.log(err, err.stack); // an error occurred
                else console.log(data);           // successful response
            });
        }
        
        recCount = recCount + 1;
        records.push(module.exports.createRecord(data));

        cache.setKey(constants.KEY_RECORDS, records);
        cache.setKey(constants.KEY_RECORDS_COUNT, recCount);
              
       
    }
}

