from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route('/')
def index():
    return '<h1>Test App</h1><form method="POST" action="/submit"><input name="test"><button type="submit">Submit</button></form>'

@app.route('/submit', methods=['POST'])
def submit():
    test_value = request.form.get('test')
    return f'Received: {test_value}'

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)