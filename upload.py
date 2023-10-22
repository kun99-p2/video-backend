from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import boto3

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
            video_filename = secure_filename(request.form['title'])
            #s3.put_object(ACL='public-read', Body=uploaded_file, Key=video_filename, Metadata=metadata)
            s3.upload_fileobj(uploaded_file, bucket, uname+"/"+video_filename, ExtraArgs={'ACL': 'public-read', 'Metadata': metadata})
            return jsonify({'success': True, 'message': 'Video uploaded successfully'}), 200
    except Exception as e:
        print(e)
        return jsonify({'success':False,'error': 'Error uploading video'}), 500
    
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
        return jsonify({'success':False,'error': 'Error fetching videos'}), 500
    
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
        return jsonify({'success':False,'error': 'Error fetching videos'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)