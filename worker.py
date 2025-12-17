import os
import json
import logging
import requests
import tempfile
import shutil
import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pathlib import Path

import yandexcloud
from ydb.iam import iam_token_source
from ydb import Driver
import boto3
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
from moviepy.editor import VideoFileClip

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
S3_ENDPOINT = os.environ.get('S3_ENDPOINT', 'https://storage.yandexcloud.net')

# YDB Driver
driver = Driver(
    endpoint=DB_ENDPOINT,
    database=DB_PATH,
    credentials=iam_token_source.IamTokenSource(
        iam_key_id=SA_KEY_ID,
        iam_key_secret=SA_SECRET,
    )
)

# S3 Client for Object Storage
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=SA_KEY_ID,
    aws_secret_access_key=SA_SECRET
)

# Yandex Cloud SDK
yc_sdk = yandexcloud.SDK(iam_token_source=iam_token_source.IamTokenSource(
    iam_key_id=SA_KEY_ID,
    iam_key_secret=SA_SECRET,
))

def update_task_status(task_id, status, error_message=None, pdf_url=None):
    """Update task status in database"""
    try:
        session = driver.table_client.session().create()

        if error_message:
            query = """
            DECLARE $id AS String;
            DECLARE $status AS String;
            DECLARE $error_message AS String;
            DECLARE $updated_at AS String;

            UPDATE tasks
            SET status = $status, error_message = $error_message, updated_at = $updated_at
            WHERE id = $id
            """
            params = {
                '$id': task_id,
                '$status': status,
                '$error_message': error_message,
                '$updated_at': datetime.utcnow().isoformat()
            }
        elif pdf_url:
            query = """
            DECLARE $id AS String;
            DECLARE $status AS String;
            DECLARE $pdf_url AS String;
            DECLARE $updated_at AS String;

            UPDATE tasks
            SET status = $status, pdf_url = $pdf_url, updated_at = $updated_at
            WHERE id = $id
            """
            params = {
                '$id': task_id,
                '$status': status,
                '$pdf_url': pdf_url,
                '$updated_at': datetime.utcnow().isoformat()
            }
        else:
            query = """
            DECLARE $id AS String;
            DECLARE $status AS String;
            DECLARE $updated_at AS String;

            UPDATE tasks
            SET status = $status, updated_at = $updated_at
            WHERE id = $id
            """
            params = {
                '$id': task_id,
                '$status': status,
                '$updated_at': datetime.utcnow().isoformat()
            }

        session.transaction().execute(query, params, commit_tx=True)
        session.close()
        logger.info(f"Updated task {task_id} status to {status}")
    except Exception as e:
        logger.error(f"Failed to update task {task_id}: {e}")

def validate_yandex_disk_url(url):
    """Validate Yandex Disk public URL"""
    try:
        parsed = urlparse(url)

        # Check if it's a valid Yandex Disk URL
        if 'disk.yandex.ru' not in parsed.netloc:
            return False, "Not a Yandex Disk URL"

        # Try to get public link info
        response = requests.head(url, timeout=10, allow_redirects=True)

        if response.status_code != 200:
            return False, f"URL returned status {response.status_code}"

        return True, "Valid URL"
    except Exception as e:
        return False, f"URL validation error: {str(e)}"

def download_video(url, temp_dir):
    """Download video from Yandex Disk"""
    try:
        # For Yandex Disk public links, we need to get the direct download URL
        # This is a simplified approach - in production, you might need to use Yandex Disk API
        response = requests.get(url, timeout=30, stream=True)

        if response.status_code != 200:
            raise Exception(f"Failed to download video: status {response.status_code}")

        # Check content type
        content_type = response.headers.get('content-type', '')
        if 'video' not in content_type.lower():
            logger.warning(f"Content type is not video: {content_type}")

        # Save to temp file
        video_path = os.path.join(temp_dir, f"video_{uuid.uuid4().hex}.mp4")

        with open(video_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Check if file exists and has content
        if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            raise Exception("Downloaded video file is empty")

        logger.info(f"Video downloaded to {video_path}, size: {os.path.getsize(video_path)} bytes")
        return video_path

    except Exception as e:
        raise Exception(f"Video download failed: {str(e)}")

def extract_audio(video_path, temp_dir):
    """Extract audio from video file"""
    try:
        audio_path = os.path.join(temp_dir, f"audio_{uuid.uuid4().hex}.wav")

        # Use moviepy to extract audio
        with VideoFileClip(video_path) as video:
            audio = video.audio
            audio.write_audiofile(audio_path, codec='pcm_s16le', verbose=False, logger=None)

        # Check if audio file was created
        if not os.path.exists(audio_path):
            raise Exception("Failed to create audio file")

        logger.info(f"Audio extracted to {audio_path}")
        return audio_path

    except Exception as e:
        raise Exception(f"Audio extraction failed: {str(e)}")

def speech_to_text(audio_path):
    """Convert speech to text using Yandex SpeechKit"""
    try:
        # Import Yandex SpeechKit
        from yandex.cloud.ai.stt.v2 import stt_service_pb2
        from yandex.cloud.ai.stt.v2 import stt_service_pb2_grpc

        # Create gRPC stub
        grpc_stub = stt_service_pb2_grpc.SpeechToTextStub(yc_sdk._grpc_channel)

        # Read audio file
        with open(audio_path, 'rb') as f:
            audio_data = f.read()

        # Prepare request
        request = stt_service_pb2.LongRunningRecognizeRequest(
            spec=stt_service_pb2.RecognitionSpec(
                language_code='ru-RU',
                profanity_filter=False,
                model='general',
                audio_encoding='LINEAR16_PCM',
                sample_rate_hertz=16000,
                audio_channel_count=1,
            ),
            audio=stt_service_pb2.RecognitionAudio(
                audio_content=audio_data
            )
        )

        # Send request
        response = grpc_stub.Recognize(request)

        # Wait for operation to complete
        operation = yc_sdk.wait_operation(response)

        # Get response
        result = stt_service_pb2.LongRunningRecognizeResponse()
        operation.result.Unpack(result)

        # Extract text from chunks
        text_chunks = []
        for chunk in result.chunks:
            for alternative in chunk.alternatives:
                text_chunks.append(alternative.text)

        full_text = ' '.join(text_chunks)

        if not full_text.strip():
            raise Exception("No speech detected in audio")

        logger.info(f"Speech to text completed, text length: {len(full_text)}")
        return full_text

    except Exception as e:
        raise Exception(f"Speech to text conversion failed: {str(e)}")

def generate_summary(text):
    """Generate summary using YandexGPT"""
    try:
        # Import YandexGPT
        from yandex.cloud.ai.gpt.v2 import gpt_service_pb2
        from yandex.cloud.ai.gpt.v2 import gpt_service_pb2_grpc

        # Create gRPC stub
        grpc_stub = gpt_service_pb2_grpc.GrpcServiceStub(yc_sdk._grpc_channel)

        # Prepare request for summarization
        prompt = f"""Сделай краткое и структурированное изложение следующего текста лекции:

{text}

Сделай изложение в виде основных тезисов и выводов."""

        request = gpt_service_pb2.CompletionRequest(
            model_uri=f"gpt://{FOLDER_ID}/yandexgpt-lite",
            completion_options=gpt_service_pb2.CompletionOptions(
                max_tokens=2000,
                temperature=0.3,
            ),
            messages=[
                gpt_service_pb2.Message(
                    role="system",
                    text="Ты - ассистент, который создает краткие и структурированные изложения лекций."
                ),
                gpt_service_pb2.Message(
                    role="user",
                    text=prompt
                )
            ]
        )

        # Send request
        response = grpc_stub.Completion(request)

        # Get the generated text
        if response.alternatives:
            summary = response.alternatives[0].message.text
        else:
            raise Exception("No response from YandexGPT")

        if not summary.strip():
            raise Exception("Empty summary generated")

        logger.info(f"Summary generated, length: {len(summary)}")
        return summary

    except Exception as e:
        raise Exception(f"Summary generation failed: {str(e)}")

def create_pdf(title, text, summary, temp_dir):
    """Create PDF with lecture content"""
    try:
        pdf_path = os.path.join(temp_dir, f"lecture_{uuid.uuid4().hex}.pdf")

        # Create PDF
        doc = SimpleDocTemplate(pdf_path, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []

        # Title
        title_style = styles['Title']
        title_style.fontSize = 24
        title_style.spaceAfter = 30
        story.append(Paragraph(title, title_style))

        # Summary section
        summary_title = Paragraph("<b>Краткое изложение</b>", styles['Heading1'])
        story.append(summary_title)
        story.append(Spacer(1, 12))

        # Add summary paragraphs
        summary_text = summary.replace('\n', '<br/>')
        summary_para = Paragraph(summary_text, styles['Normal'])
        story.append(summary_para)

        # Full text section
        story.append(Spacer(1, 20))
        full_text_title = Paragraph("<b>Полный текст лекции</b>", styles['Heading1'])
        story.append(full_text_title)
        story.append(Spacer(1, 12))

        # Add full text in chunks to avoid very long paragraphs
        chunk_size = 500  # characters
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i+chunk_size]
            chunk_text = chunk.replace('\n', '<br/>')
            chunk_para = Paragraph(chunk_text, styles['Normal'])
            story.append(chunk_para)

        # Build PDF
        doc.build(story)

        # Check if PDF was created
        if not os.path.exists(pdf_path):
            raise Exception("Failed to create PDF file")

        logger.info(f"PDF created at {pdf_path}")
        return pdf_path

    except Exception as e:
        raise Exception(f"PDF creation failed: {str(e)}")

def upload_to_storage(pdf_path, task_id):
    """Upload PDF to Object Storage"""
    try:
        # Generate unique object key
        object_key = f"lecture_notes/{task_id}.pdf"

        # Upload file
        with open(pdf_path, 'rb') as f:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=f,
                ContentType='application/pdf'
            )

        # Generate URL (this would be a presigned URL in production)
        # For now, we'll return the object key that can be used to construct a URL
        pdf_url = f"https://{BUCKET_NAME}.storage.yandexcloud.net/{object_key}"

        logger.info(f"PDF uploaded to storage: {object_key}")
        return pdf_url

    except Exception as e:
        raise Exception(f"Storage upload failed: {str(e)}")

def cleanup_temp_files(temp_dir, age_hours=1):
    """Clean up temporary files older than specified age"""
    try:
        now = datetime.now()
        for file_path in Path(temp_dir).glob('*'):
            if file_path.is_file():
                file_age = now - datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_age > timedelta(hours=age_hours):
                    file_path.unlink()
                    logger.info(f"Cleaned up old temp file: {file_path}")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

def process_task(task_id, title, video_url):
    """Process a single task"""
    temp_dir = tempfile.mkdtemp()

    try:
        logger.info(f"Processing task {task_id}: {title}")

        # Step 1: Update status to PROCESSING
        update_task_status(task_id, 'PROCESSING')

        # Step 2: Validate Yandex Disk URL
        is_valid, validation_message = validate_yandex_disk_url(video_url)
        if not is_valid:
            raise Exception(f"Invalid URL: {validation_message}")

        # Step 3: Download video
        video_path = download_video(video_url, temp_dir)

        # Step 4: Extract audio
        audio_path = extract_audio(video_path, temp_dir)

        # Step 5: Convert speech to text
        transcript = speech_to_text(audio_path)

        # Step 6: Generate summary
        summary = generate_summary(transcript)

        # Step 7: Create PDF
        pdf_path = create_pdf(title, transcript, summary, temp_dir)

        # Step 8: Upload to storage
        pdf_url = upload_to_storage(pdf_path, task_id)

        # Step 9: Update status to DONE
        update_task_status(task_id, 'DONE', pdf_url=pdf_url)

        logger.info(f"Task {task_id} completed successfully")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} failed: {error_msg}")
        update_task_status(task_id, 'ERROR', error_message=error_msg)

    finally:
        # Cleanup temp files
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup temp directory: {e}")

        # Cleanup old temp files
        cleanup_temp_files(os.path.dirname(temp_dir))

def get_task_info(task_id):
    """Get task information from database"""
    try:
        session = driver.table_client.session().create()
        query = """
        DECLARE $id AS String;
        SELECT title, video_url FROM tasks WHERE id = $id
        """

        result_sets = session.transaction().execute(query, {'$id': task_id}, commit_tx=True)
        session.close()

        if not result_sets[0].rows:
            return None, None

        row = result_sets[0].rows[0]
        title = row['title'].get()
        video_url = row['video_url'].get()

        return title, video_url

    except Exception as e:
        logger.error(f"Failed to get task {task_id}: {e}")
        return None, None

def listen_to_queue():
    """Listen to queue for new tasks"""
    import boto3

    try:
        sqs_client = boto3.client(
            'sqs',
            endpoint_url=QUEUE_URL,
            region_name='ru-central1',
            aws_access_key_id=SA_KEY_ID,
            aws_secret_access_key=SA_SECRET
        )

        logger.info("Starting to listen to queue...")

        while True:
            try:
                # Receive message
                response = sqs_client.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=3600
                )

                if 'Messages' in response:
                    message = response['Messages'][0]

                    # Parse message
                    body = json.loads(message['Body'])
                    task_id = body.get('task_id')

                    if task_id:
                        logger.info(f"Received task {task_id} from queue")

                        # Get task info
                        title, video_url = get_task_info(task_id)

                        if title and video_url:
                            # Process the task
                            process_task(task_id, title, video_url)
                        else:
                            logger.error(f"Task {task_id} not found in database")
                    else:
                        logger.error("No task_id in message")

                    # Delete message
                    sqs_client.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                    logger.info(f"Task {task_id} processed and message deleted")
                else:
                    logger.debug("No messages in queue")

            except Exception as e:
                logger.error(f"Error processing queue message: {e}")

    except Exception as e:
        logger.error(f"Queue listener error: {e}")

if __name__ == '__main__':
    logger.info("Starting worker...")
    listen_to_queue()