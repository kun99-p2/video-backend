from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
import botocore
from datetime import datetime
import hashlib
import message_broker
import re
import tempfile

app = Flask(__name__)
CORS(app)

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

#generating presigned url to save video to
@app.route('/get_presigned_url', methods=['POST'])  
def get_presigned_url():
    try:
        #metadata values for identification and additional info
        uname = request.form['user']
        gen_id = hashlib.sha256((uname+request.form['title']).encode()).hexdigest()
        upload_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        key = "videos/"+uname+"/"+request.form['title']
        #presigned url where frontend can use to upload video to
        presigned_url = s3.generate_presigned_url(ClientMethod='put_object', Params={'Bucket': bucket,'Key': key}, ExpiresIn=900)
        print(presigned_url)
        return jsonify({'url': presigned_url, 'id': gen_id, 'datetime': upload_datetime})
    except Exception as e:
        return jsonify({'error': e}), 500

#redis queue
@app.route('/tasks', methods=['POST'])
def enqueue_tasks():
    data = request.get_json()
    key = data.get('key')
    user = data.get('user')
    message_broker.enqueue_video_tasks(key, user, data.get("title"), data.get("id"), data.get("time"))
    return 'Enqueued tasks.'

#delete video+thumbnail
@app.route('/delete', methods=['DELETE'])
def delete():
    try:
        data = request.get_json()
        username = data['username']
        title = data['title']
        id = data['id']
        #deleting video
        response = s3.list_objects_v2(Bucket=bucket, Prefix="videos/"+username+'/')
        #list all videos for a user and delete video with matching title and id from request
        for obj in response.get('Contents', []):
            obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
            if obj_key['title'] == title and obj_key['id'] == id:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                break
        #deleting thumbnail
        response_thumbnails = s3.list_objects_v2(Bucket=bucket, Prefix="thumbnail/"+username+'/')
        #list all thumbnails for a user and delete thumbnail with matching title and id from request
        for obj in response_thumbnails.get('Contents', []):
            obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
            if obj_key['title'] == title and obj_key['id'] == id:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                break
        try:
            #deleting cached video
            response_thumbnails = s3.list_objects_v2(Bucket=bucket, Prefix="cached/"+username+'/')
            #list all cached video for a user and delete cached video with matching title and id from request
            for obj in response_thumbnails.get('Contents', []):
                obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
                if obj_key['title'] == title and obj_key['id'] == id:
                    s3.delete_object(Bucket=bucket, Key=obj['Key'])
                    break
        except Exception as e:
            print("wasnt cached")
        return jsonify({'message': "Succesfully deleted " + title})
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error deleting'}), 500

#returns videos that frontend can store in a list to request for videos to view
@app.route('/videos', methods=['GET'])
def videos():
    try:
        #listing and retrieving all m3u8 files
        response = s3.list_objects(Bucket=bucket, Prefix="videos/")
        others = []
        for obj in response.get('Contents', []):
            if obj['Key'].endswith(".m3u8"):
                others.append(obj['Key'])
        #generating urls for each m3u8 file and getting their metadata
        videos = []
        for key in others:
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
            response = s3.head_object(Bucket=bucket, Key= key)
            metadata = response['Metadata']
            videos.append([{
                'file': video,
                'metadata': {
                    'title': metadata['title'],
                    'desc': metadata['desc'],
                    'user': metadata['user'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error fetching videos'}), 500

#for video playback
@app.route('/hls', methods=['POST'])
def video_chunks():
    try:
        data = request.get_json()
        m3u8_key = 'videos/'+data['user']+'/'+data['title']+'.m3u8'
        cached_key = 'cached/'+data['user']+'/'+data['title']+'.m3u8'
        try:
            response = s3.head_object(Bucket=bucket, Key=cached_key)
            url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': cached_key})
            #checking if cached file has expired
            expires_date = datetime.datetime.strptime(response.get('Expires'), "%a, %d %b %Y %H:%M:%S %Z")
            current_date = datetime.datetime.now(datetime.timezone.utc)
            #if cached file has expired delete and cache new
            if expires_date < current_date:
                s3.delete_object(Bucket=bucket, Key=cached_key)
                return cache_new(data, m3u8_key, cached_key)
            else:
                metadata = response['Metadata']
                return jsonify({'m3u8': url,'metadata': metadata})
        except Exception as e:
            #if file isnt cached, cache it
            return cache_new(data, m3u8_key, cached_key)       
    except Exception as e:
        print(str(e))  
        return jsonify({'f': e})
    
def cache_new(data, m3u8_key, cached_key):
    #create temp file to that acts as a notepad
    with tempfile.NamedTemporaryFile(mode='w+b', suffix=".mp4",delete=False) as temp_m3u8:
        try:
            s3.download_file(bucket, m3u8_key, temp_m3u8.name)
            regex = r'[a-zA-Z0-9_-]+\.ts'
            i=0
            #get all rows in m3u8 file
            with open(temp_m3u8.name, 'r') as f:
                rows = f.readlines()
            #for every .ts file generate a presigned url for it and replace the row
            with open(temp_m3u8.name, 'w') as f:
                for row in rows:
                    if re.search(regex, row):
                        ts_url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': 'videos/'+data['user']+'/'+data['title']+'_'+str(i)+'.ts'})
                        changed_ts = re.sub(regex, ts_url, row)
                        f.write(changed_ts)
                        i+=1
                    else:
                        f.write(row)
            #to store contents of the temp file in s3
            with open(temp_m3u8.name, 'r') as f:
                m3u8_content = f.read()
            response = s3.head_object(Bucket=bucket, Key= m3u8_key)
            metadata = response['Metadata']
            s3.put_object(Body=m3u8_content, Bucket=bucket, Key=cached_key, Metadata= metadata, Expires=600)
            url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': cached_key})
            return jsonify({'m3u8': url,'metadata': metadata})
        except Exception as e:
            print(str(e))
            return jsonify({'f': e}) 

#for listing all the videos available to watch (home page)
@app.route('/thumbnails', methods=['GET'])
def thumbnails():
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="thumbnail/")
        thumbnails = []
        #generating urls for the thumbnails and getting their metadata
        for obj in response.get('Contents', []):
            thumbnail = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            response = s3.head_object(Bucket=bucket, Key= obj['Key'])
            metadata = response['Metadata']
            thumbnails.append([{
                'file': thumbnail,
                'metadata': {
                    'title': metadata['title'],
                    'desc': metadata['desc'],
                    'user': metadata['user'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'thumbnails': thumbnails})
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error fetching thuumbnails'}), 500
    
@app.route('/my_thumbnails', methods=['POST'])
def user_thumbnails():
    data = request.get_json()
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="thumbnail/"+data['username']+'/')
        thumbnails = []
        for obj in response.get('Contents', []):
            thumbnail = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            response = s3.head_object(Bucket=bucket, Key= obj['Key'])
            metadata = response['Metadata']
            thumbnails.append([{
                'file': thumbnail,
                'metadata': {
                    'title': metadata['title'],
                    'desc': metadata['desc'],
                    'user': metadata['user'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'thumbnails': thumbnails})
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error fetching thumbnails'}), 500    

if __name__ == '__main__':
    app.run(debug=True, port=5001)