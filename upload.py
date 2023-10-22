from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import boto3

app = Flask(__name__)
CORS(app)

DO_SPACES_ACCESS_KEY = 'DO00ZDHLNMDBMNG3QCUH'
DO_SPACES_SECRET_KEY = 'OwFblS+RCSVMALzyiOopm+Bo5p2U672vzKF64+b996g'
DO_SPACES_ENDPOINT = 'https://ss-p2.sgp1.digitaloceanspaces.com'
DO_SPACES_BUCKET = 'ss-p2'

s3 = boto3.client('s3', endpoint_url=DO_SPACES_ENDPOINT, aws_access_key_id=DO_SPACES_ACCESS_KEY, aws_secret_access_key=DO_SPACES_SECRET_KEY)

@app.route('/upload', methods=['POST'])
def upload():
    try:
        uploaded_file = request.files['video']
        metadata = {
                'title': request.form['title'],
                'description': request.form['desc']
            }
        if uploaded_file:
            video_filename = secure_filename(uploaded_file.filename)
            s3.upload_fileobj(uploaded_file, DO_SPACES_BUCKET, video_filename, ExtraArgs={'ACL': 'public-read', 'Metadata': metadata})
            return jsonify({'success': True, 'message': 'Video uploaded successfully'}), 200
    except Exception as e:
        print(e)
        return jsonify({'success': False,'error': 'Error uploading video'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)