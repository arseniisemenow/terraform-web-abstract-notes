from flask import Flask, render_template, request, jsonify
import os
import uuid
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)

# Environment variables
FOLDER_ID = os.getenv('FOLDER_ID', 'b1g7o9d5g1b75epvb54e')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'lecture-notes-storage')
QUEUE_URL = os.getenv('QUEUE_URL', '')

# In-memory task storage (for demo purposes)
tasks = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit', methods=['POST'])
def submit_task():
    try:
        title = request.form.get('title')
        video_url = request.form.get('video_url')
        description = request.form.get('description', '')

        if not title or not video_url:
            return jsonify({
                'error': 'Please provide both title and video URL'
            }), 400

        # Generate task ID
        task_id = str(uuid.uuid4())

        # Store task data (in-memory for demo)
        tasks[task_id] = {
            'task_id': task_id,
            'title': title,
            'video_url': video_url,
            'description': description,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'progress': 0,
            'notes_url': None,
            'pdf_url': None
        }

        # For demo purposes, simulate processing
        tasks[task_id]['status'] = 'processing'
        tasks[task_id]['progress'] = 25

        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': 'Task submitted successfully!',
            'status': 'processing'
        })

    except Exception as e:
        return jsonify({
            'error': f'Failed to submit task: {str(e)}'
        }), 500

@app.route('/status/<task_id>')
def get_status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    # Simulate progress for demo
    if task['status'] == 'processing' and task['progress'] < 100:
        task['progress'] = min(task['progress'] + 10, 100)
        if task['progress'] == 100:
            task['status'] = 'completed'
            task['notes_url'] = f'/download/{task_id}/notes'
            task['pdf_url'] = f'/download/{task_id}/pdf'

    return jsonify(task)

@app.route('/download/<task_id>/<file_type>')
def download_file(task_id, file_type):
    task = tasks.get(task_id)
    if not task or task['status'] != 'completed':
        return jsonify({'error': 'File not available'}), 404

    if file_type == 'notes':
        # Return a simple text file for demo
        notes_content = f"""Lecture Notes: {task['title']}

Generated on: {task['created_at']}

Video URL: {task['video_url']}

Description: {task['description']}

📝 Summary:
This is a demo version of lecture notes generated from the video "{task['title']}".
In the full implementation, this would contain:

• Key topics covered in the lecture
• Important definitions and concepts
• Examples and explanations
• References and further reading

🎓 This demonstrates the basic functionality of the Lecture Notes Generator.
The full version will include AI-powered transcription and summarization.
"""
        response = app.response_class(
            response=notes_content,
            mimetype='text/plain',
            headers={"Content-Disposition": f"attachment; filename=notes_{task_id}.txt"}
        )
        return response

    elif file_type == 'pdf':
        # For now, return the same notes as text (PDF generation would be added later)
        return redirect(f'/download/{task_id}/notes')

    else:
        return jsonify({'error': 'Invalid file type'}), 400

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('HOST', '0.0.0.0')
    app.run(host=host, port=port, debug=False)