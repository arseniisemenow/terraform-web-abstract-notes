import os
import json
import boto3
import time
import requests
from botocore.exceptions import ClientError
from datetime import datetime

class LectureNotesWorker:
    def __init__(self):
        # Environment variables
        self.DB_ENDPOINT = os.getenv('DB_ENDPOINT')
        self.FOLDER_ID = os.getenv('FOLDER_ID')
        self.QUEUE_URL = os.getenv('QUEUE_URL')
        self.BUCKET_NAME = os.getenv('BUCKET_NAME')
        self.S3_ENDPOINT = os.getenv('S3_ENDPOINT')
        self.SA_KEY_ID = os.getenv('SA_KEY_ID')
        self.SA_SECRET = os.getenv('SA_SECRET')

        # Initialize clients
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.S3_ENDPOINT,
            aws_access_key_id=self.SA_KEY_ID,
            aws_secret_access_key=self.SA_SECRET
        )

        self.sqs_client = boto3.client(
            'sqs',
            endpoint_url=self.QUEUE_URL,
            aws_access_key_id=self.SA_KEY_ID,
            aws_secret_access_key=self.SA_SECRET,
            region_name='ru-central1'
        )

    def run(self):
        """Main worker loop"""
        print("Lecture Notes Worker started...")
        while True:
            try:
                # Poll for messages
                response = self.sqs_client.receive_message(
                    QueueUrl=self.QUEUE_URL,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )

                messages = response.get('Messages', [])
                if not messages:
                    print("No messages in queue. Waiting...")
                    time.sleep(10)
                    continue

                for message in messages:
                    print(f"Processing message: {message['MessageId']}")
                    try:
                        task_data = json.loads(message['Body'])
                        self.process_task(task_data)

                        # Delete message from queue
                        self.sqs_client.delete_message(
                            QueueUrl=self.QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"Completed task: {task_data.get('task_id')}")

                    except Exception as e:
                        print(f"Error processing task: {str(e)}")

            except Exception as e:
                print(f"Worker error: {str(e)}")
                time.sleep(30)

    def process_task(self, task_data):
        """Process a single task"""
        task_id = task_data.get('task_id')

        try:
            # Update task status to processing
            self.update_task_status(task_id, 'processing')

            # Simulate video processing and transcription
            print(f"Processing video for task {task_id}: {task_data.get('title')}")

            # Download/transcribe video (simulated)
            transcription = self.transcribe_video(task_data.get('video_url'))

            # Generate notes from transcription
            notes_content = self.generate_notes(task_data.get('title'), transcription)

            # Create PDF
            pdf_content = self.create_pdf(notes_content)

            # Upload PDF to S3
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=f'results/{task_id}/notes.pdf',
                Body=pdf_content,
                ContentType='application/pdf'
            )

            # Update task status to completed
            self.update_task_status(task_id, 'completed')

        except Exception as e:
            print(f"Task processing failed: {str(e)}")
            self.update_task_status(task_id, 'failed', str(e))

    def transcribe_video(self, video_url):
        """Simulate video transcription"""
        # In a real implementation, you would:
        # 1. Download video from URL
        # 2. Extract audio
        # 3. Use Yandex SpeechKit for transcription

        print(f"Transcribing video: {video_url}")
        time.sleep(5)  # Simulate processing time

        # Mock transcription for demo
        return f"""
        Welcome to today's lecture on Introduction to Machine Learning.

        Machine learning is a subset of artificial intelligence that enables systems
        to learn and improve from experience without being explicitly programmed.

        Key concepts we'll cover today:
        1. Supervised Learning - Learning from labeled data
        2. Unsupervised Learning - Finding patterns in unlabeled data
        3. Reinforcement Learning - Learning through rewards

        In supervised learning, we have input data X and output labels Y.
        The goal is to learn a function f that maps X to Y.

        Common algorithms include:
        - Linear Regression for continuous outputs
        - Logistic Regression for classification
        - Decision Trees for interpretable models
        - Neural Networks for complex patterns

        Model evaluation is crucial. We use metrics like accuracy, precision,
        recall, and F1-score to assess performance.

        Thank you for your attention. Any questions?
        """

    def generate_notes(self, title, transcription):
        """Generate structured notes from transcription"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        notes = f"""
LECTURE NOTES: {title}
Generated on: {timestamp}
=========================================

SUMMARY:
{transcription[:500]}...

KEY POINTS:
• Machine learning enables systems to learn from experience
• Three main types: supervised, unsupervised, and reinforcement learning
• Supervised learning uses labeled data with inputs and outputs
• Model evaluation uses metrics like accuracy, precision, recall
• Common algorithms include regression, decision trees, and neural networks

DEFINITIONS:
• Supervised Learning: Learning from labeled training data
• Unsupervised Learning: Finding patterns in unlabeled data
• Reinforcement Learning: Learning through reward-based feedback
• Model Evaluation: Assessing algorithm performance using metrics

IMPORTANT ALGORITHMS:
1. Linear Regression - Predicting continuous values
2. Logistic Regression - Binary classification
3. Decision Trees - Rule-based decision making
4. Neural Networks - Complex pattern recognition

CONCLUSION:
Machine learning provides powerful tools for extracting insights from data
and making predictions. Understanding the different types and when to apply
them is crucial for successful implementation.

=========================================
Generated by AI • Lecture Notes Generator
        """

        return notes

    def create_pdf(self, content):
        """Create a simple PDF content (mock implementation)"""
        # In a real implementation, you would use libraries like ReportLab
        # For now, we'll return the content as-is for simplicity
        return content.encode('utf-8')

    def update_task_status(self, task_id, status, error=None):
        """Update task status in S3"""
        try:
            # Get existing task data
            response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=f'tasks/{task_id}.json'
            )
            task_data = json.loads(response['Body'].read())

            # Update status
            task_data['status'] = status
            task_data['updated_at'] = datetime.now().isoformat()
            if error:
                task_data['error'] = error

            # Save updated data
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=f'tasks/{task_id}.json',
                Body=json.dumps(task_data),
                ContentType='application/json'
            )

        except Exception as e:
            print(f"Error updating task status: {str(e)}")

if __name__ == '__main__':
    worker = LectureNotesWorker()
    worker.run()