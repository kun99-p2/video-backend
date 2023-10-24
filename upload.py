from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import boto3
from moviepy.editor import VideoFileClip
import tempfile
import imageio
from datetime import datetime
import hashlib

app = Flask(__name__)
CORS(app)

access_key = 'DO00ZDHLNMDBMNG3QCUH'
secret = 'OwFblS+RCSVMALzyiOopm+Bo5p2U672vzKF64+b996g'
endpoint = 'https://sgp1.digitaloceanspaces.com'
bucket = 'ss-p2'

s3 = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret)

@app.route('/upload', methods=['POST'])
def upload():
    try:
        uploaded_file = request.files['video']
        uname = request.form['user']
        upload_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        gen_id = hashlib.sha256((uname+request.form['title']).encode()).hexdigest()
        metadata = {
            'title': request.form['title'],
            'description': request.form['desc'],
            'time': upload_datetime,
            'id': gen_id
        }
        if uploaded_file:
            #temp files to use moviepy to check video duration and create thumbnail
            temp = tempfile.NamedTemporaryFile(delete=False)
            temp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")            
            uploaded_file.save(temp)
            with VideoFileClip(temp.name) as video:
                thumbnail = video.get_frame(1)
                imageio.imwrite(temp_thumb.name, thumbnail)
                duration = video.duration
            if duration <= 60:
                video_filename = secure_filename(request.form['title'])
                #s3.put_object(ACL='public-read', Body=uploaded_file, Key=video_filename, Metadata=metadata)
                s3.upload_file(temp.name, bucket, "videos/"+uname+"/"+video_filename, ExtraArgs={'ACL': 'public-read', 'ContentType':'video/mp4', 'Metadata': metadata})
                s3.upload_file(temp_thumb.name, bucket, "thumbnail/"+uname+"/"+video_filename, ExtraArgs={'ACL': 'public-read', 'ContentType':'image/jpg', 'Metadata': metadata})
                print(metadata)
                return jsonify({'success': True, 'message': 'Video uploaded successfully', 'id': gen_id}), 200
            else:
                temp.close()
                temp_thumb.close()
                return jsonify({'success':False,'message': 'Video too long'}), 500
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error uploading video'}), 500
    
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
        return jsonify({'success':True,'message': "Succesfully deleted " + title})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error deleting'}), 500
    
@app.route('/videos', methods=['GET'])
def videos():
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="videos/")
        videos = []
        for obj in response.get('Contents', []):
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            response = s3.head_object(Bucket=bucket, Key= obj['Key'])
            metadata = response['Metadata']
            videos.append([{
                'file': video,
                'metadata': {
                    'title': metadata['title'],
                    'time': metadata['time'],
                    'id': metadata['id']
                }
            }])
        print(videos)
        return jsonify({'success':True,'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500

@app.route('/video', methods=['POST'])
def video():
    try:
        data = request.get_json()
        id = data['id']
        response = s3.list_objects(Bucket="ss-p2", Prefix="videos/")
        requested_video = []
        for obj in response.get('Contents', []):
            obj_key = s3.head_object(Bucket=bucket, Key=obj['Key'])['Metadata']
            if obj_key['id'] == id:
                video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
                response = s3.head_object(Bucket=bucket, Key= obj['Key'])
                metadata = response['Metadata']
                requested_video = ([{
                    'file': video,
                    'metadata': {
                        'title': metadata['title'],
                        'time': metadata['time'],
                        'id': metadata['id']
                    }
                }])
                break
        return jsonify({'success':True,'video': requested_video})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500

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
    
@app.route('/my_videos', methods=['POST'])
def user_videos():
    data = request.get_json()
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="videos/"+data['username']+'/')
        videos = []
        for obj in response.get('Contents', []):
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            videos.append([{
                'file': video,
                'metadata': {
                    'title': video['Metadata']['title'],
                    'time': video['Metadata']['time'],
                    'id': video['Metadata']['id']
                }
            }])
        return jsonify({'success':True,'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500    
    
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