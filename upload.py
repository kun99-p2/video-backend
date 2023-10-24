from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import boto3
from moviepy.editor import VideoFileClip
import tempfile
import io
import imageio
from datetime import datetime

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
        metadata = {
            'title': request.form['title'],
            'description': request.form['desc']
        }
        if uploaded_file:
            #temp files to use moviepy to check video duration and create thumbnail
            temp = tempfile.NamedTemporaryFile(delete=False)
            temp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
            upload_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            uploaded_file.save(temp)
            with VideoFileClip(temp.name) as video:
                thumbnail = video.get_frame(1)
                imageio.imwrite(temp_thumb.name, thumbnail)
                duration = video.duration
            if duration <= 60:
                video_filename = secure_filename(request.form['title'])
                #s3.put_object(ACL='public-read', Body=uploaded_file, Key=video_filename, Metadata=metadata)
                with open(temp.name, 'rb') as f:
                    file_contents = f.read()
                s3.upload_fileobj(io.BytesIO(file_contents), bucket, uname+"/"+video_filename, ExtraArgs={'ACL': 'public-read', 'Metadata': metadata})
                s3.upload_file(temp_thumb.name, bucket, "thumbnail/"+uname+"/"+video_filename, ExtraArgs={'ACL': 'public-read', 'Metadata': {'time': upload_datetime}})
                temp.close()
                temp_thumb.close()
                return jsonify({'success': True, 'message': 'Video uploaded successfully'}), 200
            else:
                temp.close()
                temp_thumb.close()
                return jsonify({'success':False,'message': 'Video too long'}), 500
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error uploading video'}), 500
    
@app.route('/videos', methods=['GET'])
def videos():
    try:
        response = s3.list_objects(Bucket="ss-p2")
        videos = []
        for obj in response.get('Contents', []):
            print('Object Key:', obj['Key'])
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            videos.append(video)
        return jsonify({'success':True,'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500
    
@app.route('/thumbnails', methods=['GET'])
def thumbnails():
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix="thumbnail/")
        thumbnails = []
        for obj in response.get('Contents', []):
            print('Object Key:', obj['Key'])
            thumbnail = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            thumbnails.append(thumbnail)
        return jsonify({'success':True,'thumbnails': thumbnails})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching thuumbnails'}), 500
    
@app.route('/my_videos', methods=['POST'])
def user_videos():
    data = request.get_json()
    try:
        response = s3.list_objects(Bucket="ss-p2", Prefix=data['username']+'/')
        videos = []
        print("GET ", data['username'])
        for obj in response.get('Contents', []):
            print('Object Key:', obj['Key'])
            video = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': obj['Key']})
            videos.append(video)
        return jsonify({'success':True,'videos': videos})
    except Exception as e:
        print(e)
        return jsonify({'success':False,'message': 'Error fetching videos'}), 500    

if __name__ == '__main__':
    app.run(debug=True, port=5001)