from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file
import os
import json
import uuid
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

# Environment variables
DB_ENDPOINT = os.getenv('DB_ENDPOINT')
FOLDER_ID = os.getenv('FOLDER_ID')
QUEUE_URL = os.getenv('QUEUE_URL')
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
SA_KEY_ID = os.getenv('SA_KEY_ID')
SA_SECRET = os.getenv('SA_SECRET')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=SA_KEY_ID,
    aws_secret_access_key=SA_SECRET
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit_task():
    try:
        title = request.form.get('title')
        video_url = request.form.get('video_url')

        if not title or not video_url:
            return render_template('index.html', error='Please provide both title and video URL')

        # Generate task ID
        task_id = str(uuid.uuid4())

        # Save task data to S3
        task_data = {
            'task_id': task_id,
            'title': title,
            'video_url': video_url,
            'status': 'pending',
            'created_at': str(uuid.uuid4())
        }

        # Upload task data to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f'tasks/{task_id}.json',
            Body=json.dumps(task_data),
            ContentType='application/json'
        )

        return render_template('index.html', success=f'Task submitted! Task ID: {task_id}')

    except Exception as e:
        return render_template('index.html', error=f'Error submitting task: {str(e)}')

@app.route('/download/<task_id>')
def download_result(task_id):
    try:
        # Try to download the result
        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=f'results/{task_id}/notes.pdf'
        )

        # Save to temporary file and send
        temp_file = f'/tmp/{task_id}_notes.pdf'
        with open(temp_file, 'wb') as f:
            f.write(response['Body'].read())

        return send_file(temp_file, as_attachment=True, download_name=f'{task_id}_lecture_notes.pdf')

    except s3_client.exceptions.NoSuchKey:
        return render_template('index.html', error=f'Result not found for task ID: {task_id}')
    except Exception as e:
        return render_template('index.html', error=f'Error downloading result: {str(e)}')

@app.route('/status/<task_id>')
def get_status(task_id):
    try:
        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=f'tasks/{task_id}.json'
        )
        task_data = json.loads(response['Body'].read())
        return jsonify(task_data)
    except s3_client.exceptions.NoSuchKey:
        return jsonify({'error': 'Task not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)