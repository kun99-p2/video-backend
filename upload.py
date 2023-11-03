from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
import botocore
from datetime import datetime
import hashlib
import message_broker

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

@app.route('/get_presigned_url', methods=['POST'])  
def get_presigned_url():
    try:
        uname = request.form['user']
        gen_id = hashlib.sha256((uname+request.form['title']).encode()).hexdigest()
        upload_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        key = "videos/"+uname+"/"+request.form['title']
        presigned_url = s3.generate_presigned_url(ClientMethod='put_object', Params={'Bucket': bucket,'Key': key}, ExpiresIn=900)
        print(presigned_url)
        return jsonify({'url': presigned_url, 'id': gen_id, 'datetime': upload_datetime})
    except Exception as e:
        return jsonify({'error': e}), 500

@app.route('/thumbnail', methods=['POST'])
def enqueue_thumbnail_task():
    data = request.get_json()
    key = data.get('key')
    user = data.get('user')
    message_broker.enqueue_video_tasks(key, user, data.get("title"), data.get("id"), data.get("time"))
    return 'Enqueued tasks.'
    
@app.route('/delete', methods=['DELETE'])
def delete():
    try:
        data = request.get_json()
        username = data['username']
        title = data['title']
        id = data['id']
        #deleting video
        response = s3.list_objects_v2(Bucket=bucket, Prefix="videos/"+username+'/')
        for obj in response.get('Contents', []):
            obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
            if obj_key['title'] == title and obj_key['id'] == id:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                break
        #deleting thumbnail
        response_thumbnails = s3.list_objects_v2(Bucket=bucket, Prefix="thumbnail/"+username+'/')
        for obj in response_thumbnails.get('Contents', []):
            obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
            if obj_key['title'] == title and obj_key['id'] == id:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                break
        print("SUCCESS")
        return jsonify({'success':True,'message': "Succesfully deleted " + title})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error deleting'}), 500
    
@app.route('/videos', methods=['GET'])
def videos():
    try:
        response = s3.list_objects(Bucket=bucket, Prefix="videos/")
        others = []
        for obj in response.get('Contents', []):
            if obj['Key'].endswith(".m3u8"):
                others.append(obj['Key'])
        videos = []
        print(others)
        for key in others:
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
            response = s3.head_object(Bucket=bucket, Key= key)
            metadata = response['Metadata']
            videos.append([{
                'file': video,
                'metadata': {
                    'title': metadata['title'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'success':True,'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500

#lazy sol
# @app.route('/spec/<index>', methods=['GET'])
# def specific_index(index):
#     try:
#         response = s3.list_objects(Bucket=bucket, Prefix="videos/")
#         others = []
#         for obj in response.get('Contents', []):
#             if obj['Key'].endswith(".m3u8"):
#                 others.append(obj['Key'])
#         videos = []
#         for key in others:
#             video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
#             response = s3.head_object(Bucket=bucket, Key= obj['Key'])
#             metadata = response['Metadata']
#             videos.append([{
#                 'file': video,
#                 'metadata': {
#                     'title': metadata['title'],
#                     'time': metadata['time'],
#                     'id': metadata['id']
#                 }
#             }])
#         return jsonify({'success':True,'video': videos[int(index)]})
#     except Exception as e:
#         print(e)
#         return jsonify({'success':False,'message': 'Error fetching video'}), 500

@app.route('/hls', methods=['POST'])
def video_chunks():
    try:
        data = request.get_json()
        m3u8_key = 'videos/'+data['user']+'/'+data['title']+'.m3u8'
        m3u8 = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': m3u8_key})
        response = s3.head_object(Bucket=bucket, Key= m3u8_key)
        metadata = response['Metadata']
        return jsonify({'m3u8': m3u8, 'metadata': metadata})
    except Exception as e:
        print(e)
        return jsonify({'f': 'f'})
    
# @app.route('/videos', methods=['GET'])
# def videos():
#     try:
#         response = s3.list_objects(Bucket="ss-p2", Prefix="videos/")
#         videos = []
#         for obj in response.get('Contents', []):
#             video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
#             response = s3.head_object(Bucket=bucket, Key= obj['Key'])
#             metadata = response['Metadata']
#             videos.append([{
#                 'file': video,
#                 'metadata': {
#                     'title': metadata['title'],
#                     'time': metadata['time'],
#                     'id': metadata['id']
#                 }
#             }])
#         return jsonify({'success':True,'videos': videos})
#     except Exception as e:
#         print(e)
#         return jsonify({'success':False,'message': 'Error fetching videos'}), 500

# @app.route('/video', methods=['POST'])
# def video():
#     try:
#         data = request.get_json()
#         id = data['id']
#         response = s3.list_objects(Bucket="ss-p2", Prefix="videos/")
#         requested_video = []
#         for obj in response.get('Contents', []):
#             obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
#             if obj_key['id'] == id:
#                 video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
#                 response = s3.head_object(Bucket=bucket, Key= obj['Key'])
#                 metadata = response['Metadata']
#                 requested_video = ([{
#                     'file': video,
#                     'metadata': {
#                         'title': metadata['title'],
#                         'time': metadata['time'],
#                         'id': metadata['id']
#                     }
#                 }])
#                 break
#         return jsonify({'success':True,'video': requested_video})
#     except Exception as e:
#         print(e)
#         return jsonify({'success':False,'message': 'Error fetching videos'}), 500

@app.route('/thumbnails', methods=['GET'])
def thumbnails():
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="thumbnail/")
        thumbnails = []
        for obj in response.get('Contents', []):
            thumbnail = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            response = s3.head_object(Bucket=bucket, Key= obj['Key'])
            metadata = response['Metadata']
            thumbnails.append([{
                'file': thumbnail,
                'metadata': {
                    'title': metadata['title'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'success':True,'thumbnails': thumbnails})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching thuumbnails'}), 500
    
# @app.route('/my_videos', methods=['POST'])
# def user_videos():
#     data = request.get_json()
#     try:
#         response = s3.list_objects(Bucket="ss-p2", Prefix="videos/"+data['username']+'/')
#         videos = []
#         for obj in response.get('Contents', []):
#             video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
#             videos.append([{
#                 'file': video,
#                 'metadata': {
#                     'title': video['Metadata']['title'],
#                     'time': video['Metadata']['time'],
#                     'id': video['Metadata']['id']
#                 }
#             }])
#         return jsonify({'success':True,'videos': videos})
#     except Exception as e:
#         print(e)
#         return jsonify({'success':False,'message': 'Error fetching videos'}), 500    
    
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
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        return jsonify({'success':True,'thumbnails': thumbnails})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching thumbnails'}), 500    

if __name__ == '__main__':
    app.run(debug=True, port=5001)