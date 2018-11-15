import boto3
import json
import uuid
import time
stream_name = 'twitter-stream'

kinesis_client = boto3.client('kinesis', region_name='eu-west-2')
records = [
    {
        'age': 29, 
        'stack': 'python'
    }, 
    {
        'age':30, 
        'stack':'reactjs'
    },

    {
        'age':32, 
        'stack':'Java'
    },
    {
        'age':40, 
        'stack':'Scalar'
    }
]

partition_key = str(uuid.uuid4())


def put_to_stream(records, partition_key): 
    for record in records:
        put_response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record)
            PartitionKey=partition_key
        )

        time.sleep(2)
    
