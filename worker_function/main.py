import os
import json
import uuid
import requests
import tempfile
import subprocess
import re
from datetime import datetime
import boto3
import time
from botocore.exceptions import ClientError
import logging
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
from moviepy import VideoFileClip
from urllib.parse import quote

# Debug update - Fix worker queue trigger handling - Thu Dec 18 10:45:00 AM MSK 2025
# Add math import and fallback audio extraction - Thu Dec 18 11:00:00 AM MSK 2025
# Fix IAM token generation for SpeechKit - Thu Dec 18 11:05:00 AM MSK 2025
# Fix video download with SSL handling - Thu Dec 18 11:10:00 AM MSK 2025
# Simplify IAM token approach - Thu Dec 18 11:15:00 AM MSK 2025
# Working SpeechKit implementation with fallback - Thu Dec 18 11:20:00 AM MSK 2025
# Use moviepy for MP3 conversion - Mon Dec 23 2025

# Configure logging (must be before any function that uses logger)
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

    def is_yandex_disk_link(self, url):
        """Check if URL is a Yandex Disk public link"""
        yandex_disk_patterns = [
            r'https://disk\.yandex\.[a-z]+/d/',
            r'https://yadi\.sk/d/',
            r'https://disk\.yandex\.[a-z]+/i/',
            r'https://disk\.360\.yandex\.[a-z]+/d/',
        ]
        return any(re.match(pattern, url) for pattern in yandex_disk_patterns)

    def download_yandex_disk_video(self, video_url, task_id, temp_dir, video_path):
        """Download video from Yandex Disk public link using REST API"""
        try:
            logger.info(f"Yandex Disk download started")
            logger.info(f"  Public URL: {video_url}")

            # Use Yandex Disk REST API to get direct download URL
            # The API accepts the full URL as public_key parameter (must be URL-encoded)
            encoded_key = quote(video_url, safe='')
            api_url = f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={encoded_key}"
            logger.info(f"  API call: GET /public/resources/download")
            logger.info(f"  Original video URL: {video_url}")
            logger.info(f"  Encoded public_key: {encoded_key[:100]}...")
            logger.info(f"  Full API URL: {api_url[:150]}...")

            response = requests.get(api_url, timeout=10)

            logger.info(f"  API response status: {response.status_code}")

            if response.status_code == 404:
                error_msg = response.text
                logger.error(f"  ERROR: Resource not found (404)")
                logger.error(f"  API response: {error_msg[:200]}")
                raise Exception(
                    f"Yandex Disk resource not found. This could mean:\n"
                    f"1. The link has expired or was deleted\n"
                    f"2. Invalid URL format\n"
                    f"3. Resource is private and not publicly accessible\n"
                    f"API Error: {error_msg[:200]}"
                )
            elif response.status_code != 200:
                logger.error(f"  ERROR: API returned {response.status_code}")
                logger.error(f"  API response: {response.text[:200]}")
                raise Exception(f"Yandex Disk API error: {response.status_code}")

            download_info = response.json()
            download_url = download_info.get('href')

            if not download_url:
                raise Exception("No download URL received from Yandex Disk API")

            logger.info(f"  ✓ Got direct download URL")
            logger.info(f"  Starting file download...")

            # Download the actual video file
            download_response = requests.get(
                download_url,
                stream=True,
                timeout=300,  # 5 minutes for large files
                headers={'User-Agent': 'Lecture Notes Generator'}
            )

            download_response.raise_for_status()

            # Save the file
            downloaded_size = 0
            with open(video_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)

            file_size = os.path.getsize(video_path)
            file_size_mb = file_size / (1024 * 1024)
            logger.info(f"  ✓ Download complete: {file_size:,} bytes ({file_size_mb:.2f} MB)")
            logger.info(f"  ✓ Saved to: {video_path}")

            if file_size == 0:
                raise Exception("Downloaded file is empty")

            # Return temp_dir and video_path like the original method
            return temp_dir, video_path

        except Exception as e:
            logger.error(f"Failed to download from Yandex Disk: {e}")
            raise Exception(f"Yandex Disk download failed: {e}")

    def get_iam_token(self):
        """Get IAM token for SpeechKit using multiple authentication methods"""
        try:
            logger.info("Attempting to get IAM token for SpeechKit")

            # METHOD 1: Direct OAuth token exchange (most reliable for Cloud Functions)
            yc_token = os.getenv('YC_TOKEN')
            if yc_token:
                try:
                    logger.info("Attempting OAuth token exchange for IAM token")

                    response = requests.post(
                        'https://iam.api.cloud.yandex.net/iam/v1/tokens',
                        headers={'Content-Type': 'application/json'},
                        json={'yandexPassportOauthToken': yc_token},
                        timeout=15
                    )

                    logger.info(f"IAM API response status: {response.status_code}")

                    if response.status_code == 200:
                        token_data = response.json()
                        iam_token = token_data.get('iamToken')
                        if iam_token:
                            logger.info(f"SUCCESS: Got IAM token via OAuth exchange, length: {len(iam_token)}")
                            return iam_token
                        else:
                            logger.error(f"No iamToken in response: {token_data}")
                    else:
                        logger.error(f"IAM token exchange failed: {response.status_code} - {response.text}")

                except Exception as e:
                    logger.error(f"OAuth token exchange error: {e}")

            # METHOD 2: Try using the YC CLI with service account impersonation
            service_account_id = os.getenv('SERVICE_ACCOUNT_ID')
            if service_account_id:
                try:
                    logger.info(f"Trying yc CLI with service account: {service_account_id}")

                    # Create a temporary config with the OAuth token
                    temp_config = tempfile.mktemp()
                    with open(temp_config, 'w') as f:
                        f.write(f'token: {yc_token}\n')

                    result = subprocess.run(
                        ['yc', '--config', temp_config, 'iam', 'create-token', '--service-account-id', service_account_id],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )

                    # Clean up temp config
                    try:
                        os.remove(temp_config)
                    except:
                        pass

                    if result.returncode == 0:
                        token = result.stdout.strip()
                        if token and len(token) > 50:
                            logger.info(f"SUCCESS: Got IAM token using yc CLI, length: {len(token)}")
                            return token
                        else:
                            logger.error(f"Invalid token from yc CLI: '{token}'")
                    else:
                        logger.error(f"yc CLI failed: {result.stderr}")

                except Exception as e:
                    logger.error(f"yc CLI method failed: {e}")

            # METHOD 3: Use YC_TOKEN directly if it looks like an IAM token
            if yc_token and yc_token.startswith('t1.'):
                # YC_TOKEN is already an IAM token (starts with t1.)
                logger.info(f"Using YC_TOKEN directly as IAM token, length: {len(yc_token)}")
                return yc_token

            # All methods failed
            logger.error("FAILED: All IAM token generation methods failed")
            return None

        except Exception as e:
            logger.error(f"FAILED: Error in get_iam_token: {e}")
            return None

    def download_video(self, video_url, task_id):
        """Download video from URL with enhanced error handling and Yandex Disk support"""
        try:
            logger.info(f"Starting download for task {task_id}")
            logger.info(f"  Source URL: {video_url}")

            # Create temporary directory
            temp_dir = tempfile.mkdtemp()
            video_path = os.path.join(temp_dir, f"{task_id}.mp4")

            # Check if this is a Yandex Disk link and handle it specifically
            if self.is_yandex_disk_link(video_url):
                logger.info("  → Yandex Disk link detected, using API download method")
                return self.download_yandex_disk_video(video_url, task_id, temp_dir, video_path)

            logger.info("  → Regular HTTP(S) download method")
            # Enhanced download with better error handling
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            # Try with SSL verification first, then fallback
            try:
                response = requests.get(
                    video_url,
                    headers=headers,
                    stream=True,
                    timeout=60,  # Shorter timeout
                    verify=True
                )
            except requests.exceptions.SSLError:
                logger.warning("SSL verification failed, trying without verification")
                response = requests.get(
                    video_url,
                    headers=headers,
                    stream=True,
                    timeout=60,
                    verify=False
                )

            response.raise_for_status()
            logger.info(f"Response status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}")

            # Check if we got actual content
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) == 0:
                logger.warning("Content length is 0")
            elif not content_length and response.status_code == 200:
                logger.info("No content-length header, but status is 200 - continuing")

            # Download the file
            downloaded_size = 0
            with open(video_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        downloaded_size += len(chunk)

            file_size = os.path.getsize(video_path)
            logger.info(f"Video downloaded to {video_path}, size: {file_size} bytes")

            if file_size == 0:
                logger.error(f"Downloaded file is empty: {video_path}")
                return None, None

            return video_path, temp_dir

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error downloading video: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Failed to download video: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None, None

    def convert_to_mp3(self, video_path, task_id):
        """Convert video to MP3 using moviepy"""
        try:
            logger.info("Converting video to MP3 using moviepy")

            # Define output path for MP3
            mp3_path = os.path.join(os.path.dirname(video_path) or '/tmp', f"{task_id}.mp3")

            # Load video and extract audio
            video = VideoFileClip(video_path)
            audio = video.audio

            if audio is None:
                raise Exception("Video has no audio track")

            # Write audio to MP3
            logger.info(f"Writing audio to MP3: {mp3_path}")
            audio.write_audiofile(mp3_path, codec='libmp3lame', bitrate='192k', logger=None)

            # Close video to free resources
            video.close()
            audio.close()

            # Verify the file was created
            if not os.path.exists(mp3_path):
                raise Exception(f"MP3 file was not created at {mp3_path}")

            file_size = os.path.getsize(mp3_path)
            if file_size == 0:
                raise Exception("MP3 file is empty")

            logger.info(f"MP3 conversion successful: {mp3_path} ({file_size} bytes)")
            return mp3_path

        except Exception as e:
            logger.error(f"Failed to convert to MP3: {e}")
            raise Exception(f"MP3 conversion failed: {e}")

    def transcribe_audio_speechkit(self, audio_path, task_id):
        """Transcribe audio using Yandex SpeechKit - REAL TRANSCRIPTION ONLY"""
        try:
            logger.info("Starting transcription with SpeechKit")

            # Get IAM token using the enhanced method
            iam_token = self.get_iam_token()

            if not iam_token:
                logger.error("FAILED: No IAM token available for SpeechKit")
                raise Exception("SpeechKit authentication failed - no IAM token available")

            logger.info(f"Got IAM token, length: {len(iam_token)}")

            # Read audio file
            with open(audio_path, 'rb') as f:
                audio_data = f.read()

            logger.info(f"Audio file size: {len(audio_data)} bytes")

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

            logger.info(f"SpeechKit API call - URL: {url}, Folder: {self.speechkit_folder_id}")

            logger.info(f"Sending {len(audio_data)} bytes to SpeechKit")

            response = requests.post(
                url,
                headers=headers,
                params=params,
                data=audio_data,
                timeout=600
            )

            if response.status_code == 200:
                result = response.json()
                if 'result' in result:
                    transcription = result['result']
                    logger.info(f"SUCCESS: Transcription completed: {len(transcription)} characters")
                    logger.info(f"Transcription preview: {transcription[:100]}...")
                    return transcription
                else:
                    logger.error(f"FAILED: No result in SpeechKit response: {result}")
                    raise Exception(f"SpeechKit API returned invalid response: {result}")
            else:
                logger.error(f"FAILED: SpeechKit API error: {response.status_code} - {response.text}")
                raise Exception(f"SpeechKit API failed with status {response.status_code}: {response.text}")

        except Exception as e:
            logger.error(f"FAILED: SpeechKit transcription failed: {e}")
            raise Exception(f"SpeechKit transcription error: {e}")

    def generate_service_account_token(self):
        """Generate IAM token for service account using available methods"""
        try:
            # Try to use yc command if available
            result = subprocess.run(
                ['yc', 'iam', 'create-token', '--service-account-id', os.getenv('SERVICE_ACCOUNT_ID', '')],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                token = result.stdout.strip()
                if token:
                    logger.info("Generated IAM token using yc command")
                    return token
        except Exception as e:
            logger.debug(f"yc command method failed: {e}")

        return None

    # REMOVED: create_sample_transcription method - NO MORE MOCK DATA

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
        """Process a single task - convert video to MP3"""
        try:
            task_id = task_data['task_id']
            video_url = task_data['video_url']
            title = task_data['title']
            description = task_data.get('description', '')

            logger.info(f"="*60)
            logger.info(f"PROCESSING TASK: {task_id}")
            logger.info(f"  Title: {title}")
            logger.info(f"  Video URL: {video_url}")
            logger.info(f"="*60)

            # Update task status to processing
            self.update_task_status(task_id, 'processing', 10, "Downloading video...")

            # Step 1: Download video
            video_path, temp_dir = self.download_video(video_url, task_id)
            if not video_path:
                self.update_task_status(task_id, 'failed', 0, "Failed to download video")
                return False

            # Update progress
            self.update_task_status(task_id, 'processing', 40, "Converting to MP3...")

            # Step 2: Convert to MP3
            mp3_path = self.convert_to_mp3(video_path, task_id)
            if not mp3_path:
                self.update_task_status(task_id, 'failed', 0, "Failed to convert to MP3")
                return False

            # Update progress
            self.update_task_status(task_id, 'processing', 80, "Uploading MP3...")

            # Step 3: Upload MP3 to storage
            mp3_storage_key = f"mp3/{task_id}.mp3"
            mp3_url = self.upload_to_storage(mp3_path, mp3_storage_key)

            if not mp3_url:
                self.update_task_status(task_id, 'failed', 0, "Failed to upload MP3")
                return False

            # Step 4: Update task as completed
            task_result = {
                'mp3_url': mp3_url,
                'processed_at': datetime.now().isoformat(),
                'video_duration': self.get_video_duration(video_path)
            }

            self.update_task_status(task_id, 'completed', 100, "MP3 conversion completed", task_result)

            logger.info(f"Task {task_id} completed successfully - MP3 available at: {mp3_url}")
            return True

        except Exception as e:
            logger.error(f"Error processing task: {e}")
            if 'task_id' in task_data:
                self.update_task_status(task_data['task_id'], 'failed', 0, f"Processing error: {str(e)}")
            return False

        finally:
            # Cleanup temporary files
            try:
                if 'temp_dir' in locals() and temp_dir and os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir)
                    logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to cleanup temp directory: {e}")

    def get_video_duration(self, video_path):
        """Get video duration using moviepy"""
        try:
            video = VideoFileClip(video_path)
            duration = video.duration
            video.close()
            return duration
        except:
            return None

    def process_text_with_gpt(self, transcription_text, title):
        """Process transcription text using Yandex GPT to generate structured notes"""
        try:
            logger.info("Processing transcription text with GPT...")

            # For now, create a simple structured format
            # In a real implementation, this would call Yandex GPT API
            processed_text = f"""
ЛЕКЦИЯ: {title}

КОНСПЕКТ ЛЕКЦИИ
================

ВВЕДЕНИЕ
Данный конспект составлен на основе автоматической расшифровки видеозаписи лекции.

ОСНОВНОЕ СОДЕРЖАНИЕ
{transcription_text}

ЗАКЛЮЧЕНИЕ
Конспект подготовлен автоматически с использованием технологий распознавания речи и обработки естественного языка Яндекса.

ГЕНЕРАЦИЯ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """.strip()

            return processed_text

        except Exception as e:
            logger.error(f"Failed to process text with GPT: {e}")
            # Return basic formatting if GPT processing fails
            return f"""
ЛЕКЦИЯ: {title}

КОНСПЕКТ ЛЕКЦИИ
================

{transcription_text}

ГЕНЕРАЦИЯ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """.strip()

    def generate_pdf_notes(self, processed_text, title, task_id):
        """Generate PDF from processed lecture notes"""
        try:
            logger.info("Generating PDF notes...")

            # Create PDF file path
            pdf_path = f"/tmp/{task_id}_notes.pdf"

            # Create PDF document
            doc = SimpleDocTemplate(pdf_path, pagesize=letter)
            story = []

            # Get styles
            styles = getSampleStyleSheet()
            title_style = styles['Title']
            heading_style = styles['Heading1']
            normal_style = styles['Normal']

            # Add title
            title_paragraph = Paragraph(title, title_style)
            story.append(title_paragraph)
            story.append(Spacer(1, 12))

            # Add generation timestamp
            timestamp = f"Сгенерировано: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            timestamp_paragraph = Paragraph(timestamp, normal_style)
            story.append(timestamp_paragraph)
            story.append(Spacer(1, 20))

            # Process text into paragraphs
            lines = processed_text.split('\n')
            current_section = []

            for line in lines:
                line = line.strip()
                if not line:
                    if current_section:
                        # Join current section into a paragraph
                        paragraph_text = ' '.join(current_section)
                        paragraph = Paragraph(paragraph_text, normal_style)
                        story.append(paragraph)
                        story.append(Spacer(1, 6))
                        current_section = []
                elif line.isupper() and len(line) < 50:
                    # Likely a heading
                    if current_section:
                        paragraph_text = ' '.join(current_section)
                        paragraph = Paragraph(paragraph_text, normal_style)
                        story.append(paragraph)
                        current_section = []

                    heading = Paragraph(line, heading_style)
                    story.append(heading)
                    story.append(Spacer(1, 12))
                else:
                    current_section.append(line)

            # Add any remaining text
            if current_section:
                paragraph_text = ' '.join(current_section)
                paragraph = Paragraph(paragraph_text, normal_style)
                story.append(paragraph)

            # Generate PDF
            doc.build(story)

            logger.info(f"PDF generated successfully: {pdf_path}")
            return pdf_path

        except Exception as e:
            logger.error(f"Failed to generate PDF: {e}")
            raise Exception(f"PDF generation failed: {e}")

    def save_pdf_to_storage(self, pdf_path, task_id, title):
        """Save generated PDF to object storage"""
        try:
            logger.info(f"Saving PDF to storage for task {task_id}")

            # Generate PDF filename
            pdf_filename = f"{task_id}_lecture_notes.pdf"
            storage_key = f"notes/{pdf_filename}"

            # Read PDF file
            with open(pdf_path, 'rb') as pdf_file:
                pdf_content = pdf_file.read()

            # Upload to object storage
            self.s3_client.put_object(
                Bucket=self.storage_bucket,
                Key=storage_key,
                Body=pdf_content,
                ContentType='application/pdf',
                Metadata={
                    'task_id': task_id,
                    'title': title,
                    'generated_at': datetime.now().isoformat()
                }
            )

            # Generate public URL
            pdf_url = f"https://storage.yandexcloud.net/{self.storage_bucket}/{storage_key}"

            logger.info(f"PDF saved successfully: {pdf_url}")
            return pdf_url

        except Exception as e:
            logger.error(f"Failed to save PDF to storage: {e}")
            raise Exception(f"PDF storage failed: {e}")


def handler(event, context):
    """Main handler for Yandex Cloud Functions"""
    logger.info("Worker function triggered")
    logger.info(f"Event structure: {str(event)[:200]}...")

    try:
        worker = LectureNotesWorker()
        logger.info("Worker initialized")

        # Handle triggered messages from queue (new approach)
        if 'messages' in event:
            logger.info(f"Processing {len(event['messages'])} triggered messages")

            for message in event['messages']:
                logger.info(f"Message details: {message.get('details', {})}")

                # Extract task data from message
                message_body = message['details']['message']['body']
                task_data = json.loads(message_body)
                task_id = task_data.get('task_id')

                logger.info(f"Processing triggered task: {task_id}")

                # Process the task
                success = worker.process_task(task_data)

                if success:
                    logger.info(f"Task {task_id} completed successfully")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': f'Task {task_id} processed successfully',
                            'status': 'success'
                        })
                    }
                else:
                    logger.error(f"Task {task_id} failed")
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'message': f'Task {task_id} processing failed',
                            'status': 'failed'
                        })
                    }

        # Fallback: try polling the queue directly (original approach)
        logger.info("No triggered messages, trying queue polling")
        task_data, receipt_handle = worker.get_task_from_queue()

        if task_data:
            logger.info(f"Received task from queue: {task_data['task_id']}")
            success = worker.process_task(task_data)

            if success:
                logger.info(f"Task {task_data['task_id']} completed successfully")
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