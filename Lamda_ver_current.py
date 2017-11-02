import boto3
import os
import time
import datetime
import json

# Initiate Firehose client
firehose_client = boto3.client('firehose')

DATETIME_INPUT_FORMATS = (
    '%Y-%m-%dT%H:%M:%S',
)

DATETIME_OUTPUT_FORMAT = '%Y-%m-%d %H:%M:%S'

def parse_datetime(source_string, formats=DATETIME_INPUT_FORMATS):
    parsed = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(source_string, fmt)
        except ValueError:
            continue
        else:
            break
    return parsed

def lambda_handler(event, context):
    records = []
    batch   = []
    try :
        for record in event['Records']:
            event = {}
			create_dt = parse_datetime(record['dynamodb']['NewImage']['CreationDateTime']['S'])
			update_dt = parse_datetime(record['dynamodb']['NewImage']['UpdateDateTime']['S'])
			t_stats = '{ "mac":"%s", "Accepted":"%s", "Cloud_connected":"%s", "CreationDateTime":"%s", "DeviceId":"%s", "EventName":"%s", "UpdateDateTime":"%s", "UpdatedCnt":"%s", "PF_port":"%s", "PF_status":"%s", "Proxy":"%s", "external_ip":"%s", "Admin_flag":"%s" }\n' \
                      % (record['dynamodb']['NewImage']['mac']['S'], \
                    	record['dynamodb']['NewImage']['Accepted']['N'], \
        				record['dynamodb']['NewImage']['Cloud_connected']['S'], \
        				"CreationDateTime": create_dt.strptime(DATETIME_OUTPUT_FORMAT), \
                        record['dynamodb']['NewImage']['DeviceId']['S'], \
                        record['dynamodb']['NewImage']['EventName']['S'], \
                        "UpdateDateTime": update_dt.strptime(DATETIME_OUTPUT_FORMAT), \
                        record['dynamodb']['NewImage']['UpdatedCnt']['N'], \
                        record['dynamodb']['NewImage']['PF_port']['S'], \
                        record['dynamodb']['NewImage']['PF_status']['S'], \
                        record['dynamodb']['NewImage']['Proxy']['S'], \
                        record['dynamodb']['NewImage']['external_ip']['S'], \
                        record['dynamodb']['NewImage']['Admin_flag']['N']) 
            event["Data"] = json.dumps(t_stats)
			records.append(event)
        batch.append(records)
        res = firehose_client.put_record_batch(
            DeliveryStreamName = os.environ['firehose_stream_name'],
            Records = batch[0]
        )
        print 4
        return 'Successfully processed {} records.'.format(len(event['Records']))
    except Exception as e: print(e)