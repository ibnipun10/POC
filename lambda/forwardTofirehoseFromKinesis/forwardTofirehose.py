from __future__ import print_function

import base64
import json
import boto3

print('Loading function')

DeliveryStreamName = 'SAMSAnalytics-Delivery'
client = boto3.client('firehose')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    Records = []
    data = ''
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        data = payload + '\n'
        
    p = {'Data': data }
    Records.append(p)
    print('writing to firehose')
    response = client.put_record_batch(DeliveryStreamName=DeliveryStreamName, Records=Records)
    print("Wrote the following records to Firehose: " + str(len(Records)))
    return 'Successfully processed {} records.'.format(len(event['Records']))