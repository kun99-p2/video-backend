from thumbnail_extractor.task import extract_thumbnail
import boto3
import botocore
from redis import Redis
from rq import Queue

redis_conn = Redis(host='localhost', port=6379, db=0)
firstQueue = Queue('p-t', connection=redis_conn)
secondQueue = Queue(connection=redis_conn)

access_key = 'DO00JQGULATEWKWZYCHA'
secret = '5rpGncSUAkl0BCo0E63FBy5FR3EO/daTuwxZPvOcp+8'
endpoint = 'https://sgp1.digitaloceanspaces.com'
bucket = 'ss-p2'
session = boto3.session.Session()
s3 = session.client('s3',
                        config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                        region_name='sgp1',
                        endpoint_url='https://sgp1.digitaloceanspaces.com',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret)

def enqueue_video_tasks(key, tb_key, title, id):
    #firstQueue is for processing and thumbnail extraction.
    firstQueue.enqueue(extract_thumbnail, {'key': key, 'to': tb_key, 'title': title, 'id': id})
    #firstQueue.enqueue(process_video)
    #secondQueue waits for all the tasks in firstQueue to finish before starting.
    #secondQueue.enqueue(chunk_video)
