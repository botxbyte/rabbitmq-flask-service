from flask import Blueprint, request, jsonify, render_template, Response
import os
from datetime import datetime
import collections
from app.config.logger import LoggerSetup

log_bp = Blueprint('log', __name__)

# Create an instance of LoggerSetup
logger_setup = LoggerSetup()

# Use setup_logger instead of setup_worker_logger for route files
logger = logger_setup.setup_logger()

class LogRoutes:
    @staticmethod
    def get_log_path(pid=None):
        """Get the log file path for a worker"""
        current_date = datetime.now()
        year = str(current_date.year)
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        base_path = os.path.join("app", "static", "logs", "workers", year, month, day)
        os.makedirs(base_path, exist_ok=True)

        filename = "worker"
        if pid:
            filename += f"_{pid}"
        filename += ".log"

        log_path = os.path.join(base_path, filename)
        logger.info(f"Log path generated: {log_path}")

        return log_path

    @staticmethod
    def get_log_file_link(pid=None):
        """Get the full URL and static path for a log file"""
        log_path = LogRoutes.get_log_path(pid)
        log_filename = os.path.basename(log_path)

        full_path = os.path.abspath(log_path)
        relative_path = os.path.relpath(log_path, start=os.path.join("app", "static"))
        static_url = f"/static/{relative_path.replace(os.sep, '/')}"
        full_url = request.host_url.rstrip('/') + static_url

        return full_path, static_url, full_url

    @staticmethod
    @log_bp.route("/workers/logs/<pid>", methods=["GET"])
    def tail_logs(pid):
        try:
            lines = request.args.get('lines', default=100, type=int)
            log_path = LogRoutes.get_log_path(pid)
            
            if not os.path.exists(log_path):
                return jsonify({"error": "Log file not found"}), 404
                
            with open(log_path, 'r') as f:
                log_lines = collections.deque(f, lines)
                
            return jsonify({
                "pid": pid,
                "log_file": log_path,
                "lines": list(log_lines)[::-1]
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @staticmethod
    @log_bp.route("/workers/view-logs/<pid>")
    def view_logs(pid):
        """Render the log viewer page"""
        return render_template('log_viewer.html')

    @staticmethod
    @log_bp.route("/workers/stream_logs/<pid>")
    def stream_logs(pid):
        """Stream logs using server-sent events"""
        def generate():
            try:
                log_path = LogRoutes.get_log_path(pid)
                logger.info(f"Streaming logs from: {log_path}")
                
                if not os.path.exists(log_path):
                    yield "data: Log file not found\n\n"
                    return

                with open(log_path, 'r') as f:
                    f.seek(0, os.SEEK_END)

                    while True:
                        line = f.readline()
                        if line:
                            yield f"data: {line.strip()}\n\n"
                        else:
                            import time
                            time.sleep(0.5)

            except Exception as e:
                yield f"data: Error reading logs: {str(e)}\n\n"

        return Response(generate(), mimetype='text/event-stream')
