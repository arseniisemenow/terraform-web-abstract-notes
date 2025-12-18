import os
import json
import uuid
import requests
import tempfile
import subprocess
from datetime import datetime
import boto3
import time
from botocore.exceptions import ClientError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LectureNotesWorker:
    def __init__(self):
        self.ydb_endpoint = os.getenv('YDB_ENDPOINT')
        self.ydb_database = os.getenv('YDB_DATABASE')
        self.storage_bucket = os.getenv('STORAGE_BUCKET')
        self.storage_access_key = os.getenv('STORAGE_ACCESS_KEY')
        self.storage_secret_key = os.getenv('STORAGE_SECRET_KEY')
        self.speechkit_folder_id = os.getenv('FOLDER_ID')
        self.queue_url = os.getenv('QUEUE_URL')

        # Initialize Yandex Storage client
        self.s3_client = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=self.storage_access_key,
            aws_secret_access_key=self.storage_secret_key,
            region_name='ru-central1'
        )

        # Initialize SQS client for message queue
        self.sqs_client = boto3.client(
            'sqs',
            endpoint_url='https://message-queue.api.cloud.yandex.net',
            aws_access_key_id=self.storage_access_key,
            aws_secret_access_key=self.storage_secret_key,
            region_name='ru-central1'
        )

        logger.info("Worker initialized successfully")

    def get_iam_token(self):
        """Get IAM token for SpeechKit API calls"""
        try:
            # Use Yandex Cloud CLI to get token
            result = subprocess.run(
                ['yc', 'iam', 'create-token'],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get IAM token: {e}")
            return None

    def download_video(self, video_url, task_id):
        """Download video from URL"""
        try:
            logger.info(f"Downloading video from {video_url}")

            # Create temporary directory
            temp_dir = tempfile.mkdtemp()
            video_path = os.path.join(temp_dir, f"{task_id}.mp4")

            # Download video using requests (supporting redirects)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(video_url, headers=headers, stream=True, timeout=300)
            response.raise_for_status()

            with open(video_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Video downloaded to {video_path}")
            return video_path, temp_dir

        except Exception as e:
            logger.error(f"Failed to download video: {e}")
            return None, None

    def extract_audio(self, video_path, task_id):
        """Extract audio from video using ffmpeg"""
        try:
            logger.info("Extracting audio from video")

            audio_path = os.path.join(os.path.dirname(video_path), f"{task_id}.wav")

            # Use ffmpeg to extract audio
            cmd = [
                'ffmpeg', '-i', video_path,
                '-vn',  # No video
                '-acodec', 'pcm_s16le',  # 16-bit PCM
                '-ar', '16000',  # 16kHz sample rate
                '-ac', '1',  # Mono
                '-y',  # Overwrite output file
                audio_path
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"FFmpeg error: {result.stderr}")
                return None

            logger.info(f"Audio extracted to {audio_path}")
            return audio_path

        except Exception as e:
            logger.error(f"Failed to extract audio: {e}")
            return None

    def transcribe_audio_speechkit(self, audio_path, task_id):
        """Transcribe audio using Yandex SpeechKit"""
        try:
            logger.info("Starting transcription with SpeechKit")

            # Get IAM token
            iam_token = self.get_iam_token()
            if not iam_token:
                raise Exception("Failed to get IAM token")

            # Read audio file
            with open(audio_path, 'rb') as f:
                audio_data = f.read()

            # SpeechKit API endpoint
            url = "https://stt.api.cloud.yandex.net/speech/v1/stt:recognize"

            headers = {
                'Authorization': f'Bearer {iam_token}',
                'Content-Type': 'audio/x-wav'
            }

            params = {
                'folderId': self.speechkit_folder_id,
                'lang': 'auto',  # Auto-detect language
                'format': 'lpcm',
                'sampleRateHertz': 16000
            }

            logger.info(f"Sending {len(audio_data)} bytes to SpeechKit")

            response = requests.post(
                url,
                headers=headers,
                params=params,
                data=audio_data,
                timeout=600
            )

            if response.status_code != 200:
                logger.error(f"SpeechKit API error: {response.status_code} - {response.text}")
                return None

            result = response.json()

            if 'result' not in result:
                logger.error(f"No transcription result: {result}")
                return None

            transcription = result['result']
            logger.info(f"Transcription completed: {len(transcription)} characters")

            return transcription

        except Exception as e:
            logger.error(f"Failed to transcribe audio: {e}")
            return None

    def upload_to_storage(self, file_path, object_name):
        """Upload file to Yandex Object Storage"""
        try:
            logger.info(f"Uploading {file_path} to storage as {object_name}")

            self.s3_client.upload_file(
                file_path,
                self.storage_bucket,
                object_name
            )

            # Generate public URL
            file_url = f"https://storage.yandexcloud.net/{self.storage_bucket}/{object_name}"

            logger.info(f"File uploaded successfully: {file_url}")
            return file_url

        except Exception as e:
            logger.error(f"Failed to upload to storage: {e}")
            return None

    def update_task_status(self, task_id, status, progress, message, result=None):
        """Update task status in persistent storage"""
        try:
            # Get current task data from S3
            response = self.s3_client.get_object(
                Bucket=self.storage_bucket,
                Key=f'tasks/{task_id}.json'
            )
            task_data = json.loads(response['Body'].read().decode('utf-8'))

            # Update task status
            task_data['status'] = status
            task_data['progress'] = progress
            task_data['status_message'] = message

            if result:
                task_data.update(result)

            # Save updated task back to S3
            self.s3_client.put_object(
                Bucket=self.storage_bucket,
                Key=f'tasks/{task_id}.json',
                Body=json.dumps(task_data),
                ContentType='application/json'
            )

            logger.info(f"Task {task_id}: {status} ({progress}%) - {message}")

        except Exception as e:
            logger.error(f"Failed to update task status: {e}")

    def get_task_from_queue(self):
        """Get task from message queue"""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=3600
            )

            if 'Messages' in response and response['Messages']:
                message = response['Messages'][0]
                task_data = json.loads(message['Body'])
                receipt_handle = message['ReceiptHandle']

                return task_data, receipt_handle
            else:
                return None, None

        except Exception as e:
            logger.error(f"Failed to get task from queue: {e}")
            return None, None

    def delete_message_from_queue(self, receipt_handle):
        """Delete processed message from queue"""
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted from queue")
        except Exception as e:
            logger.error(f"Failed to delete message from queue: {e}")

    def process_task(self, task_data):
        """Process a single transcription task"""
        try:
            task_id = task_data['task_id']
            video_url = task_data['video_url']
            title = task_data['title']
            description = task_data.get('description', '')

            logger.info(f"Processing task {task_id}: {title}")

            # Update task status to processing
            self.update_task_status(task_id, 'processing', 10, "Downloading video...")

            # Step 1: Download video
            video_path, temp_dir = self.download_video(video_url, task_id)
            if not video_path:
                self.update_task_status(task_id, 'failed', 0, "Failed to download video")
                return False

            # Update progress
            self.update_task_status(task_id, 'processing', 30, "Extracting audio...")

            # Step 2: Extract audio
            audio_path = self.extract_audio(video_path, task_id)
            if not audio_path:
                self.update_task_status(task_id, 'failed', 0, "Failed to extract audio")
                return False

            # Update progress
            self.update_task_status(task_id, 'processing', 50, "Transcribing audio...")

            # Step 3: Transcribe with SpeechKit
            transcription = self.transcribe_audio_speechkit(audio_path, task_id)
            if not transcription:
                self.update_task_status(task_id, 'failed', 0, "Failed to transcribe audio")
                return False

            # Update progress
            self.update_task_status(task_id, 'processing', 80, "Saving results...")

            # Step 4: Save transcription to storage
            transcript_object = f"transcriptions/{task_id}.txt"
            transcript_content = f"""Lecture: {title}

Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Description: {description}

Video URL: {video_url}

--- TRANSCRIPTION ---

{transcription}

--- END OF TRANSCRIPTION ---
"""

            # Write transcription to temporary file
            transcript_file = os.path.join(temp_dir, f"{task_id}_transcription.txt")
            with open(transcript_file, 'w', encoding='utf-8') as f:
                f.write(transcript_content)

            # Upload to storage
            transcript_url = self.upload_to_storage(transcript_file, transcript_object)

            # Step 5: Update task as completed
            task_result = {
                'transcription': transcription,
                'transcript_url': transcript_url,
                'processed_at': datetime.now().isoformat(),
                'video_duration': self.get_video_duration(video_path)
            }

            self.update_task_status(task_id, 'completed', 100, "Processing completed", task_result)

            logger.info(f"Task {task_id} completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error processing task: {e}")
            if 'task_id' in task_data:
                self.update_task_status(task_data['task_id'], 'failed', 0, f"Processing error: {str(e)}")
            return False

        finally:
            # Cleanup temporary files
            try:
                if 'temp_dir' in locals() and os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir)
                    logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to cleanup temp directory: {e}")

    def get_video_duration(self, video_path):
        """Get video duration using ffmpeg"""
        try:
            cmd = [
                'ffprobe', '-v', 'error', '-show_entries',
                'format=duration', '-of', 'csv=p=0', video_path
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return float(result.stdout.strip())
            return None
        except:
            return None


def handler(event, context):
    """Main handler for Yandex Cloud Functions"""
    logger.info("Worker function triggered")

    try:
        worker = LectureNotesWorker()
        logger.info("Worker initialized, starting to process tasks from queue")

        # Process one task from queue
        task_data, receipt_handle = worker.get_task_from_queue()

        if task_data:
            logger.info(f"Received task: {task_data['task_id']}")

            # Process the task
            success = worker.process_task(task_data)

            if success:
                logger.info(f"Task {task_data['task_id']} completed successfully")
                # Delete message from queue after successful processing
                worker.delete_message_from_queue(receipt_handle)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'Task {task_data["task_id"]} processed successfully',
                        'status': 'success'
                    })
                }
            else:
                logger.error(f"Task {task_data['task_id']} processing failed")
                # Don't delete message - let it return to queue for retry
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': f'Task {task_data["task_id"]} processing failed',
                        'status': 'failed'
                    })
                }
        else:
            # No tasks in queue
            logger.info("No tasks in queue")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No tasks in queue',
                    'status': 'idle'
                })
            }

    except Exception as e:
        logger.error(f"Worker error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': str(e),
                'status': 'error'
            })
        }

if __name__ == '__main__':
    # For local testing
    worker = LectureNotesWorker()

    # Example task for testing
    example_task = {
        'task_id': str(uuid.uuid4()),
        'title': 'Test Lecture',
        'video_url': 'https://example.com/test.mp4',
        'description': 'Test description'
    }

    worker.process_task(example_task)