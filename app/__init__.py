from flask import Flask

def create_app():
    app = Flask(__name__)
    from app.routes import queue_bp
    app.register_blueprint(queue_bp)
    return app