from thumbnail_extractor import task
from video_converter import converter
from chunker import chunker
import boto3
import botocore
from redis import Redis
from rq import Queue

redis_conn = Redis(host='localhost', port=6379, db=0)
firstQueue = Queue('q', connection=redis_conn)

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

def enqueue_video_tasks(key, user, title, id, time):
    input = {'key': key, 'user': user, 'title': title, 'id': id, 'time': time}
    firstQueue.enqueue(task.extract_thumbnail, input)
    firstQueue.enqueue(converter.convert, input)
    firstQueue.enqueue(chunker.chunker, input)