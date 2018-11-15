import boto3
import json
import uuid
import time
stream_name = 'twitter-stream'

kinesis_client = boto3.client('kinesis', region_name='eu-west-2')

# iterator

# shard id
# describe the stream
stream_description = kinesis_client.describe_stream(StreamName=stream_name)
print(f'\n{stream_description}')
# get the shard ID
shard_id = stream_description['StreamDescription']['Shards'][0]['ShardId']
print(f' \nShard ID: {shard_id}')

# get iterator
shard_iterator = kinesis_client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType='TRIM_HORIZON'
)

# iterator hash 
shard_iterator_key = shard_iterator['ShardIterator']

# data required to work this
record_response = kinesis_client.get_records(ShardIterator=shard_iterator_key, Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'])
    print(f'\n \n {record_response}')
    time.sleep(5)