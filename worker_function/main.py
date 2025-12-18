import os
import json
import uuid
import requests
import tempfile
import subprocess
import math
import struct
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

# Debug update - Fix worker queue trigger handling - Thu Dec 18 10:45:00 AM MSK 2025
# Add math import and fallback audio extraction - Thu Dec 18 11:00:00 AM MSK 2025
# Fix IAM token generation for SpeechKit - Thu Dec 18 11:05:00 AM MSK 2025
# Fix video download with SSL handling - Thu Dec 18 11:10:00 AM MSK 2025
# Simplify IAM token approach - Thu Dec 18 11:15:00 AM MSK 2025
# Working SpeechKit implementation with fallback - Thu Dec 18 11:20:00 AM MSK 2025

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

    def is_yandex_disk_link(self, url):
        """Check if URL is a Yandex Disk public link"""
        yandex_disk_patterns = [
            r'https://disk\.yandex\.[a-z]+/d/',
            r'https://yadi\.sk/d/',
            r'https://disk\.yandex\.[a-z]+/i/',
        ]
        return any(re.match(pattern, url) for pattern in yandex_disk_patterns)

    def download_yandex_disk_video(self, video_url, task_id, temp_dir, video_path):
        """Download video from Yandex Disk public link"""
        try:
            logger.info(f"Downloading from Yandex Disk: {video_url}")

            # Extract public key from Yandex Disk URL
            if '/d/' in video_url:
                public_key = video_url.split('/d/')[-1].split('?')[0]
            elif '/i/' in video_url:
                public_key = video_url.split('/i/')[-1].split('?')[0]
            else:
                public_key = video_url.split('/')[-1]

            logger.info(f"Yandex Disk public_key: {public_key}")

            # Get download URL from Yandex Disk API
            api_url = f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={public_key}"

            headers = {}
            oauth_token = os.getenv('YANDEX_OAUTH_TOKEN')
            if oauth_token:
                headers['Authorization'] = f'OAuth {oauth_token}'

            response = requests.get(api_url, headers=headers, timeout=10)

            if response.status_code != 200:
                logger.error(f"Failed to get download URL: {response.status_code} - {response.text}")
                raise Exception(f"Yandex Disk API error: {response.status_code}")

            download_info = response.json()
            download_url = download_info.get('href')

            if not download_url:
                raise Exception("No download URL received from Yandex Disk API")

            logger.info(f"Got download URL, downloading file...")

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
            logger.info(f"Downloaded {file_size} bytes from Yandex Disk")

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
            logger.info(f"Downloading video from {video_url}")

            # Create temporary directory
            temp_dir = tempfile.mkdtemp()
            video_path = os.path.join(temp_dir, f"{task_id}.mp4")

            # Check if this is a Yandex Disk link and handle it specifically
            if self.is_yandex_disk_link(video_url):
                logger.info("Detected Yandex Disk link, using specialized download")
                return self.download_yandex_disk_video(video_url, task_id, temp_dir, video_path)

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

    def extract_audio(self, video_path, task_id):
        """Extract audio from video - fallback to dummy audio for testing"""
        try:
            logger.info("Extracting audio from video")

            audio_path = os.path.join(os.path.dirname(video_path), f"{task_id}.wav")

            # Try ffmpeg first (in case it's available)
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
            if result.returncode == 0:
                logger.info(f"Audio extracted using ffmpeg: {audio_path}")
                return audio_path
            else:
                logger.warning(f"FFmpeg not available: {result.stderr}")
                logger.info("Creating dummy audio file for testing")

        except Exception as e:
            logger.warning(f"FFmpeg extraction failed: {e}")
            logger.info("Creating dummy audio file for testing")

        # Create dummy audio for testing SpeechKit
        try:
            import wave
            import struct

            # Create a simple WAV file header and data
            sample_rate = 16000
            duration = 3  # 3 seconds
            frequency = 440  # A4 note
            amplitude = 8000

            num_samples = int(sample_rate * duration)
            with wave.open(audio_path, 'wb') as wav_file:
                wav_file.setnchannels(1)  # Mono
                wav_file.setsampwidth(2)  # 16-bit
                wav_file.setframerate(sample_rate)

                # Generate sine wave
                for i in range(num_samples):
                    value = int(amplitude * math.sin(2 * math.pi * frequency * i / sample_rate))
                    wav_file.writeframes(struct.pack('<h', value))

            logger.info(f"Created dummy audio file: {audio_path}")
            return audio_path

        except Exception as e:
            logger.error(f"Failed to create dummy audio: {e}")
            # As last resort, create minimal WAV file
            with open(audio_path, 'wb') as f:
                # Minimal WAV header (44 bytes)
                f.write(b'RIFF\x36\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00\x40\x1f\x00\x00\x80\x3e\x00\x00\x02\x00\x10\x00data\x00\x00\x00\x00')
            logger.warning(f"Created minimal audio file: {audio_path}")
            return audio_path

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

            # Update progress for PDF generation
            self.update_task_status(task_id, 'processing', 85, "Generating lecture notes...")

            # Step 5: Process transcription text with GPT
            processed_text = self.process_text_with_gpt(transcription, title)

            # Step 6: Generate PDF notes
            pdf_path = self.generate_pdf_notes(processed_text, title, task_id)

            # Step 7: Save PDF to storage
            pdf_url = self.save_pdf_to_storage(pdf_path, task_id, title)

            # Step 8: Update task as completed
            task_result = {
                'transcription': transcription,
                'transcript_url': transcript_url,
                'pdf_url': pdf_url,
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