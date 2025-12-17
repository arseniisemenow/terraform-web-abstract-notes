from flask import Flask, render_template_string, request, jsonify, send_file
import os
import uuid
from datetime import datetime

app = Flask(__name__)

# Simple HTML template (same as before)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎓 Lecture Notes Generator</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .container {
            background: white;
            padding: 40px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 10px;
            font-size: 2.5em;
        }
        .subtitle {
            text-align: center;
            color: #666;
            margin-bottom: 30px;
            font-size: 1.2em;
        }
        .form-group {
            margin-bottom: 25px;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #555;
        }
        input[type="text"], input[type="url"], textarea {
            width: 100%;
            padding: 15px;
            border: 2px solid #e1e5e9;
            border-radius: 8px;
            font-size: 16px;
            box-sizing: border-box;
            transition: border-color 0.3s;
        }
        input[type="text"]:focus, input[type="url"]:focus, textarea:focus {
            outline: none;
            border-color: #667eea;
        }
        textarea {
            height: 120px;
            resize: vertical;
        }
        .submit-btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 8px;
            font-size: 18px;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            transition: transform 0.2s;
        }
        .submit-btn:hover {
            transform: translateY(-2px);
        }
        .result {
            margin-top: 30px;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        .download-btn {
            background: #28a745;
            color: white;
            padding: 10px 20px;
            text-decoration: none;
            border-radius: 5px;
            margin-right: 10px;
            display: inline-block;
        }
        .error {
            color: #dc3545;
            background: #f8d7da;
            border-left-color: #dc3545;
        }
        .success {
            color: #155724;
            background: #d4edda;
            border-left-color: #28a745;
        }
        .progress {
            width: 100%;
            height: 20px;
            background: #f0f0f0;
            border-radius: 10px;
            overflow: hidden;
            margin: 10px 0;
        }
        .progress-bar {
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s ease;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎓 Lecture Notes Generator</h1>
        <p class="subtitle">Transform video lectures into organized notes with AI</p>

        {% if message %}
        <div class="result {{ 'success' if success else 'error' }}">
            {{ message }}
            {% if task_id %}
            <div class="progress">
                <div class="progress-bar" style="width: {{ progress }}%"></div>
            </div>
            <p><strong>Progress:</strong> {{ progress }}%</p>
            <p><strong>Task ID:</strong> {{ task_id }}</p>
            {% if download_links %}
                <p><strong>Download:</strong></p>
                {% for link in download_links %}
                    <a href="{{ link.url }}" class="download-btn">{{ link.label }}</a>
                {% endfor %}
            {% endif %}
            <button onclick="checkProgress()" class="submit-btn" style="margin-top: 10px;">Check Progress</button>
            {% endif %}
        </div>
        {% endif %}

        <form method="POST">
            <div class="form-group">
                <label for="title">📚 Lecture Title:</label>
                <input type="text" id="title" name="title" placeholder="e.g., Introduction to Machine Learning" required>
            </div>

            <div class="form-group">
                <label for="video_url">🎥 Video URL:</label>
                <input type="url" id="video_url" name="video_url" placeholder="https://youtube.com/watch?v=..." required>
            </div>

            <div class="form-group">
                <label for="description">📝 Description (Optional):</label>
                <textarea id="description" name="description" placeholder="Additional details about the lecture..."></textarea>
            </div>

            <button type="submit" class="submit-btn">🚀 Generate Lecture Notes</button>
        </form>
    </div>

    {% if task_id %}
    <script>
        function checkProgress() {
            fetch('/status/{{ task_id }}')
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'completed') {
                        window.location.reload();
                    } else {
                        document.querySelector('.progress-bar').style.width = data.progress + '%';
                        document.querySelector('p strong').textContent = 'Progress: ' + data.progress + '%';
                    }
                })
                .catch(error => console.error('Error:', error));
        }

        // Auto-refresh every 3 seconds if processing
        {% if status == 'processing' %}
        setInterval(checkProgress, 3000);
        {% endif %}
    </script>
    {% endif %}
</body>
</html>
"""

# In-memory task storage
tasks = {}

@app.route('/')
def index():
    message = request.args.get('message')
    success = request.args.get('success', 'false') == 'true'
    task_id = request.args.get('task_id')
    status = request.args.get('status')
    progress = request.args.get('progress', '0')

    download_links = []
    if task_id and status == 'completed':
        download_links = [
            {'label': '📄 Download Notes (TXT)', 'url': f'/download/{task_id}/notes'},
            {'label': '📋 Download Notes (PDF)', 'url': f'/download/{task_id}/pdf'}
        ]

    return render_template_string(HTML_TEMPLATE,
                                 message=message,
                                 success=success,
                                 task_id=task_id,
                                 status=status,
                                 progress=progress,
                                 download_links=download_links)

@app.route('/submit', methods=['POST'])
def submit_task():
    title = request.form.get('title')
    video_url = request.form.get('video_url')
    description = request.form.get('description', '')

    if not title or not video_url:
        error_message = "Please provide both title and video URL"
        return redirect(f'/?message={error_message}&success=false')

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        'task_id': task_id,
        'title': title,
        'video_url': video_url,
        'description': description,
        'status': 'processing',
        'created_at': datetime.now().isoformat(),
        'progress': 25
    }

    return redirect(f'/?message=Lecture submitted successfully! Processing started...&success=true&task_id={task_id}&status=processing&progress=25')

@app.route('/status/<task_id>')
def get_status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    # Simulate progress
    if task['status'] == 'processing' and task['progress'] < 100:
        task['progress'] = min(task['progress'] + 15, 100)
        if task['progress'] == 100:
            task['status'] = 'completed'

    return jsonify(task)

@app.route('/download/<task_id>/<file_type>')
def download_file(task_id, file_type):
    task = tasks.get(task_id)
    if not task or task['status'] != 'completed':
        return jsonify({'error': 'File not available'}), 404

    if file_type == 'notes':
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
The full version will include AI-powered transcription and summarization using Yandex Cloud SpeechKit and YandexGPT.

Features implemented:
• Web interface for submitting lecture URLs
• Task status tracking with progress indicators
• Download functionality for generated notes in multiple formats
• Cloud-based processing architecture

Next steps:
• Integrate with Yandex SpeechKit for video transcription
• Add YandexGPT for intelligent summarization
• Implement PDF generation with ReportLab
• Add file storage to Yandex Object Storage
"""

        response = app.response_class(
            response=notes_content,
            mimetype='text/plain',
            headers={"Content-Disposition": f"attachment; filename=notes_{task_id}.txt"}
        )
        return response

    elif file_type == 'pdf':
        # For now, redirect to text version
        return redirect(f'/download/{task_id}/notes')

    return jsonify({'error': 'Invalid file type'}), 400

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0.0',
        'app_type': 'Flask Application'
    })

if __name__ == '__main__':
    # Production deployment for Yandex Cloud
    app.run(host='0.0.0.0', port=8080, debug=False)