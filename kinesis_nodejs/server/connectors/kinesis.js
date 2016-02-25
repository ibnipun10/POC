var AWS = require('aws-sdk'),
    constants = require('../../common/utils/constants');

var kinesis = new AWS.Kinesis({ region : constants.KINESIS_REGION });

module.exports = {
    
    convertToJson: function (validparams) {    
        return JSON.stringify(validparams)
    },

    writeToKinesis: function(data, streamName) {
        var randomNumber = Math.floor(Math.random() * 100000);
        var streamData = 'data-' + data;
        var partitionKey = 'pk-' + randomNumber;
        var recordParams = {
            Data: streamData,
            PartitionKey: partitionKey,
            StreamName: streamName
        };
    
        kinesis.putRecord(recordParams, function (err, data) {
            if (err) {
                console.error(err);
            }
        });
    }
}

