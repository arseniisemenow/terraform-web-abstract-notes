import os
import uuid
import json
import logging
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, make_response
from werkzeug.urls import url_parse
import yandexcloud
from ydb.iam import iam_token_source
from ydb import Driver

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DB_ENDPOINT = os.environ.get('DB_ENDPOINT')
DB_PATH = os.environ.get('DB_PATH')
SA_KEY_ID = os.environ.get('SA_KEY_ID')
SA_SECRET = os.environ.get('SA_SECRET')
FOLDER_ID = os.environ.get('FOLDER_ID')
QUEUE_URL = os.environ.get('QUEUE_URL')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
API_GATEWAY_URL = os.environ.get('API_GATEWAY_URL')

# YDB Driver
driver = Driver(
    endpoint=DB_ENDPOINT,
    database=DB_PATH,
    credentials=iam_token_source.IamTokenSource(
        iam_key_id=SA_KEY_ID,
        iam_key_secret=SA_SECRET,
    )
)

def init_db():
    """Initialize the database table"""
    try:
        driver.wait(timeout=5, fail_fast=True)
        query = """
        CREATE TABLE IF NOT EXISTS tasks (
            id String,
            title String,
            video_url String,
            status String,
            error_message String?,
            pdf_url String?,
            created_at String,
            updated_at String,
            PRIMARY KEY (id)
        )
        """
        session = driver.table_client.session().create()
        session.execute_scheme(query)
        session.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

def create_task(title, video_url):
    """Create a new task"""
    task_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    try:
        session = driver.table_client.session().create()
        query = """
        DECLARE $id AS String;
        DECLARE $title AS String;
        DECLARE $video_url AS String;
        DECLARE $status AS String;
        DECLARE $created_at AS String;
        DECLARE $updated_at AS String;

        INSERT INTO tasks (id, title, video_url, status, created_at, updated_at)
        VALUES ($id, $title, $video_url, $status, $created_at, $updated_at)
        """

        params = {
            '$id': task_id,
            '$title': title,
            '$video_url': video_url,
            '$status': 'QUEUED',
            '$created_at': now,
            '$updated_at': now
        }

        session.transaction().execute(query, params, commit_tx=True)
        session.close()

        # Send to queue
        send_to_queue(task_id)

        return task_id
    except Exception as e:
        logger.error(f"Failed to create task: {e}")
        raise

def get_all_tasks():
    """Get all tasks sorted by creation time (newest first)"""
    try:
        session = driver.table_client.session().create()
        query = """
        DECLARE $limit AS Uint64;
        SELECT id, title, video_url, status, error_message, pdf_url, created_at, updated_at
        FROM tasks
        ORDER BY created_at DESC
        LIMIT $limit
        """

        result_sets = session.transaction().execute(query, {'$limit': 1000}, commit_tx=True)
        session.close()

        tasks = []
        for row in result_sets[0].rows:
            task = {
                'id': row['id'].get(),
                'title': row['title'].get(),
                'video_url': row['video_url'].get(),
                'status': row['status'].get(),
                'error_message': row['error_message'].get() if row['error_message'].get() else None,
                'pdf_url': row['pdf_url'].get() if row['pdf_url'].get() else None,
                'created_at': row['created_at'].get(),
                'updated_at': row['updated_at'].get()
            }
            tasks.append(task)

        return tasks
    except Exception as e:
        logger.error(f"Failed to get tasks: {e}")
        return []

def send_to_queue(task_id):
    """Send task to processing queue"""
    try:
        import boto3

        client = boto3.client(
            'sqs',
            endpoint_url=QUEUE_URL,
            region_name='ru-central1',
            aws_access_key_id=SA_KEY_ID,
            aws_secret_access_key=SA_SECRET
        )

        message_body = json.dumps({'task_id': task_id})

        client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message_body
        )

        logger.info(f"Task {task_id} sent to queue")
    except Exception as e:
        logger.error(f"Failed to send task to queue: {e}")

@app.route('/')
def index():
    """Main page with form and task list"""
    tasks = get_all_tasks()
    return render_template('index.html', tasks=tasks)

@app.route('/submit', methods=['POST'])
def submit():
    """Submit a new task"""
    title = request.form.get('title', '').strip()
    video_url = request.form.get('video_url', '').strip()

    if not title or not video_url:
        return redirect(url_for('index'))

    try:
        task_id = create_task(title, video_url)
        logger.info(f"Created task {task_id}")
    except Exception as e:
        logger.error(f"Failed to create task: {e}")

    return redirect(url_for('index'))

@app.route('/download/<task_id>')
def download(task_id):
    """Download PDF for a completed task"""
    try:
        session = driver.table_client.session().create()
        query = """
        DECLARE $id AS String;
        SELECT pdf_url FROM tasks WHERE id = $id
        """

        result_sets = session.transaction().execute(query, {'$id': task_id}, commit_tx=True)
        session.close()

        if not result_sets[0].rows:
            return redirect(url_for('index'))

        pdf_url = result_sets[0].rows[0]['pdf_url'].get()

        if not pdf_url:
            return redirect(url_for('index'))

        return redirect(pdf_url)

    except Exception as e:
        logger.error(f"Failed to download PDF: {e}")
        return redirect(url_for('index'))

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8080, debug=True)