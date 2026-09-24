from flask import Flask, request, jsonify, send_from_directory
from databricks.sdk import WorkspaceClient
import os

app = Flask(__name__, static_folder='static')
w = WorkspaceClient()

# Your deployed supervisor agent endpoint name
AGENT_ENDPOINT = "mas-56ab9e62-endpoint"

@app.route('/')
def index():
    """Serve the main chat interface"""
    return send_from_directory('static', 'index.html')

@app.route('/api/ask', methods=['POST'])
def ask_agent():
    """Query the supervisor agent with a question"""
    try:
        data = request.json
        question = data.get('question', '')
        
        if not question:
            return jsonify({"error": "No question provided"}), 400
        
        # Query the supervisor agent endpoint
        response = w.serving_endpoints.query(
            name=AGENT_ENDPOINT,
            messages=[{"role": "user", "content": question}]
        )
        
        # Extract the answer
        answer = response.choices[0].message.content
        
        return jsonify({
            "answer": answer,
            "status": "success"
        })
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
