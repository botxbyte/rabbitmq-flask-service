# app/routes.py
from flask import Flask, send_from_directory
import os
from app.routes.queue_routes import queue_bp
from app.routes.worker_routes import worker_bp
from app.routes.log_routes import log_bp

# Set up static folder for serving static files
STATIC_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
app = Flask(__name__, static_folder=STATIC_FOLDER)

# Register blueprints with URL prefixes
app.register_blueprint(queue_bp, url_prefix='/queue')
app.register_blueprint(worker_bp, url_prefix='/worker')
app.register_blueprint(log_bp, url_prefix='/logs')

@app.route('/static/<path:filename>')
def custom_static(filename):
    """Serve static files"""
    return send_from_directory(STATIC_FOLDER, filename)