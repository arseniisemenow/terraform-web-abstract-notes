import http.server
import socketserver
import os

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        html = '''
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
        .features {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 40px;
        }
        .feature {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 10px;
        }
        .feature-icon {
            font-size: 2em;
            margin-bottom: 10px;
        }
        .status {
            background: #d4edda;
            color: #155724;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #c3e6cb;
            margin-bottom: 20px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎓 Lecture Notes Generator</h1>
        <p class="subtitle">Transform video lectures into organized notes with AI</p>

        <div class="status">
            ✅ <strong>Application Successfully Deployed!</strong><br>
            Your infrastructure is running on Yandex Cloud
        </div>

        <form>
            <div class="form-group">
                <label for="title">📚 Lecture Title:</label>
                <input type="text" id="title" placeholder="e.g., Introduction to Machine Learning" required>
            </div>

            <div class="form-group">
                <label for="video_url">🎥 Video URL:</label>
                <input type="url" id="video_url" placeholder="https://youtube.com/watch?v=..." required>
            </div>

            <div class="form-group">
                <label for="description">📝 Description (Optional):</label>
                <textarea id="description" placeholder="Additional details about the lecture..."></textarea>
            </div>

            <button type="submit" class="submit-btn">🚀 Generate Lecture Notes</button>
        </form>

        <div class="features">
            <div class="feature">
                <div class="feature-icon">🤖</div>
                <h3>AI-Powered</h3>
                <p>Advanced transcription and summarization</p>
            </div>
            <div class="feature">
                <div class="feature-icon">⚡</div>
                <h3>Fast Processing</h3>
                <p>Quick turnaround for your notes</p>
            </div>
            <div class="feature">
                <div class="feature-icon">📄</div>
                <h3>PDF Export</h3>
                <p>Download notes in various formats</p>
            </div>
            <div class="feature">
                <div class="feature-icon">☁️</div>
                <h3>Cloud Storage</h3>
                <p>Secure storage on Yandex Cloud</p>
            </div>
        </div>
    </div>

    <script>
        document.querySelector('form').addEventListener('submit', function(e) {
            e.preventDefault();
            alert('🎉 Demo mode: Application successfully deployed!\\n\\nNext steps:\\n1. Build custom Docker images\\n2. Push to Yandex Container Registry\\n3. Update Terraform configuration\\n4. Deploy your full application');
        });
    </script>
</body>
</html>
        '''

        self.wfile.write(html.encode())

PORT = 8080
if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), MyHTTPRequestHandler) as httpd:
        print(f"🚀 Server running on port {PORT}")
        httpd.serve_forever()