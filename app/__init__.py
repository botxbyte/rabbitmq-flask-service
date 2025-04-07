from flask import Flask
from app.routes.queue_routes import queue_bp
from app.routes.worker_routes import worker_bp
from app.routes.log_routes import log_bp

def create_app():
    app = Flask(__name__)
    
    # Register all blueprints with their URL prefixes
    app.register_blueprint(queue_bp, url_prefix='/queue')
    app.register_blueprint(worker_bp, url_prefix='/worker')
    app.register_blueprint(log_bp, url_prefix='/logs')
    
    return app