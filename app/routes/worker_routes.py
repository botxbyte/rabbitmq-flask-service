from flask import Blueprint, request, jsonify
import multiprocessing
import time
import requests
import os
from pathlib import Path
import importlib
import inspect
import threading
from app.workers.base import BaseWorker
from app.rabbitmq import get_rabbitmq_connection
from app.config.logger import LoggerSetup
from app.config.config import RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT
from app.routes.log_routes import LogRoutes

# Create an instance of LoggerSetup
logger_setup = LoggerSetup()

# Use setup_logger for route files
logger = logger_setup.setup_logger()
RABBITMQ_AUTH = (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

# Global worker registry
worker_processes = {}

# Active workers tracking
active_workers_lock = threading.Lock()
active_workers = []

# Create blueprint with url_prefix
worker_bp = Blueprint('worker', __name__)

# Register routes at module level
@worker_bp.route('/scale/<queue_name>', methods=['POST'])
def scale_workers(queue_name):
    return WorkerRoutes.scale_queue_workers(queue_name)

@worker_bp.route('/<queue_name>', methods=['GET'])
def get_workers(queue_name):
    return WorkerRoutes.get_queue_workers(queue_name)

@worker_bp.route('/workers', methods=['POST'])
def find_workers():
    return WorkerRoutes.find_queue_workers()

class WorkerRoutes:
    """Worker management routes"""
    
    @staticmethod
    def get_available_workers():
        """Dynamically load all worker classes from the workers directory"""
        workers = {}
        workers_dir = Path(__file__).parent.parent / 'workers'
        
        worker_files = [f for f in os.listdir(workers_dir) if f.endswith('.py') and f != '__init__.py' and f != 'base.py']
        
        for file in worker_files:
            module_name = os.path.splitext(file)[0]
            try:
                module = importlib.import_module(f'app.workers.{module_name}')
                
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, BaseWorker) and 
                        obj != BaseWorker):
                        worker_name = module_name.lower()
                        workers[worker_name] = obj
                        
            except Exception as e:
                logger.error(f"Error loading worker module {module_name}: {str(e)}")
        
        return workers

    @staticmethod
    def start_worker(queue_name, worker_name):
        try:
            logger.info(f"Starting worker {worker_name} for queue {queue_name}")
            
            # Create a new channel for this worker
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            logger.info("Created new channel")
            
            # Declare the queue (will not create if it already exists)
            channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"Declared queue: {queue_name}")
            
            # Get the worker class based on worker_name
            available_workers = WorkerRoutes.get_available_workers()
            logger.info(f"Available workers: {list(available_workers.keys())}")
            
            worker_class = available_workers.get(worker_name)
            if not worker_class:
                error_msg = f"Invalid worker name: {worker_name}. Available workers: {list(available_workers.keys())}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"Found worker class: {worker_class.__name__}")
            
            # Create and start the worker with the original queue name
            worker = worker_class(channel, queue_name)
            logger.info("Worker instance created")
            
            # Set the worker name
            worker.worker_name = worker_name
            
            # Add a delay before starting the worker to prevent processing old messages
            time.sleep(2)  # Wait for 2 seconds before starting
            
            # Start the worker
            worker.start()
            logger.info("Worker started")
            
            # Log worker start
            process_id = os.getpid()
            logger.info(f"Started {worker_name} with PID: {process_id}, queue: {queue_name}")
            
            # Keep the process running
            while True:
                time.sleep(1)
                if not worker.is_alive():
                    error_msg = f"Worker {worker_name} stopped unexpectedly"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            
        except Exception as e:
            logger.error(f"Error starting worker: {str(e)}")
            raise

    @staticmethod
    def scale_queue_workers(queue_name):
        try:
            data = request.json
            if not data:
                return jsonify({"error": "Request body is required"}), 400

            desired_count = data.get("count")
            worker_name = data.get("worker_name")

            if desired_count is None:
                return jsonify({"error": "count is required"}), 400
            
            try:
                desired_count = int(desired_count)
                if desired_count < 0:
                    return jsonify({"error": "count must be non-negative"}), 400
            except ValueError:
                return jsonify({"error": "count must be a valid integer"}), 400

            # Get available worker classes
            available_workers = WorkerRoutes.get_available_workers()
            if not worker_name or worker_name not in available_workers:
                return jsonify({
                    "error": "Invalid worker name",
                    "available_names": list(available_workers.keys())
                }), 400

            # Verify queue exists
            try:
                connection = get_rabbitmq_connection()
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, passive=True)
                channel.close()
                connection.close()
            except Exception as e:
                return jsonify({"error": f"Queue '{queue_name}' does not exist"}), 404

            # Get current workers from RabbitMQ API
            url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            response = requests.get(url, auth=RABBITMQ_AUTH)
            
            if response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), response.status_code

            # Get current workers with matching worker_name prefix for the original queue
            current_workers = []
            for consumer in response.json():
                consumer_tag = consumer.get("consumer_tag", "")
                queue = consumer.get("queue", {}).get("name", "")
                
                # Check if this is our worker by looking at the consumer tag format: worker_name_pid
                parts = consumer_tag.split('_')
                if len(parts) >= 2 and parts[0] == worker_name and queue == queue_name:
                    current_workers.append(consumer)

            current_count = len(current_workers)
            logger.info(f"Found {current_count} existing workers for {worker_name} on {queue_name}")

            # Scale down if needed
            if current_count > desired_count:
                for worker in current_workers[:current_count - desired_count]:
                    try:
                        consumer_tag = worker.get("consumer_tag")
                        if not consumer_tag:
                            continue
                            
                        pid = int(consumer_tag.split('_')[1]) if '_' in consumer_tag else None
                        
                        if pid:
                            try:
                                os.kill(pid, 9)  # Force kill the process
                                logger.info(f"Terminated worker process {pid}")
                            except ProcessLookupError:
                                logger.info(f"Process {pid} already terminated")
                            except Exception as e:
                                logger.error(f"Error killing process {pid}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error removing worker: {str(e)}")

            # Scale up if needed
            workers_added = 0
            if current_count < desired_count:
                for _ in range(desired_count - current_count):
                    try:
                        process = multiprocessing.Process(
                            target=WorkerRoutes.start_worker,
                            args=(queue_name, worker_name)
                        )
                        process.daemon = False
                        process.start()
                        
                        # Wait for worker to start and verify it's running
                        time.sleep(3)  # Increased wait time to ensure proper startup
                        
                        if not process.is_alive():
                            # Check if process exited with error
                            if process.exitcode != 0:
                                raise Exception(f"Worker process failed to start (exit code: {process.exitcode})")
                            
                        workers_added += 1
                        logger.info(f"Started worker process {process.pid}")
                    except Exception as e:
                        logger.error(f"Error starting worker: {str(e)}")

            # Get final state from RabbitMQ API
            verify_response = requests.get(url, auth=RABBITMQ_AUTH)
            final_workers = []
            
            if verify_response.status_code == 200:
                # Get final workers with matching worker_name prefix
                for consumer in verify_response.json():
                    consumer_tag = consumer.get("consumer_tag", "")
                    queue = consumer.get("queue", {}).get("name", "")
                    
                    # Check if this is our worker using the original queue name
                    parts = consumer_tag.split('_')
                    if len(parts) >= 2 and parts[0] == worker_name and queue == queue_name:
                        pid = int(parts[1]) if len(parts) > 1 else None
                        final_workers.append({
                            "consumer_tag": consumer_tag,
                            "pid": pid,
                            "worker_status": "active",
                            "queue": queue,
                            "worker_name": worker_name
                        })

            final_count = len(final_workers)
            # Consider the scaling successful if we added the requested number of workers
            success = workers_added == (desired_count - current_count)

            return jsonify({
                "queue_name": queue_name,
                "worker_name": worker_name,
                "previous_count": current_count,
                "current_count": final_count,
                "target_count": desired_count,
                "workers": final_workers,
                "workers_added": workers_added,
                "workers_removed": max(0, current_count - final_count),
                "status": "scaled" if success else "partial_scale",
                "success": success,
                "message": "Workers scaled successfully" if success else "Some workers failed to scale"
            }), 200

        except Exception as e:
            logger.error(f"Error scaling workers: {str(e)}")
            return jsonify({
                "error": str(e),
                "queue_name": queue_name,
                "workers_added": 0,
                "status": "failed",
                "message": f"Failed to scale workers: {str(e)}"
            }), 500

    @staticmethod
    def get_queue_workers(queue_name):
        try:
            url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            response = requests.get(url, auth=RABBITMQ_AUTH)
            
            if response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), response.status_code
            
            consumers = response.json()
            queue_workers = []
            
            for consumer in consumers:
                if consumer.get("queue", {}).get("name") == queue_name:
                    consumer_tag = consumer.get("consumer_tag")
                    worker_pid = None
                    if '_' in consumer_tag:
                        try:
                            worker_pid = int(consumer_tag.split('_')[1])
                        except (IndexError, ValueError):
                            logger.error(f"Could not extract PID from consumer tag: {consumer_tag}")
                    
                    worker_info = {
                        "consumer_tag": consumer_tag,
                        "channel_details": consumer.get("channel_details"),
                        "connection_details": consumer.get("connection_details", {}),
                        "pid": worker_pid
                    }
                    queue_workers.append(worker_info)
            
            return jsonify({
                "queue_name": queue_name,
                "worker_count": len(queue_workers),
                "workers": queue_workers
            }), 200
            
        except Exception as e:
            logger.error(f"Error getting workers: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @staticmethod
    def find_queue_workers():
        try:
            data = request.json
            queue_name = data.get("queue_name")
            
            if not queue_name:
                return jsonify({"error": "queue_name is required"}), 400

            url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            response = requests.get(url, auth=RABBITMQ_AUTH)
            
            if response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), response.status_code
            
            consumers = response.json()
            queue_workers = []
            
            for consumer in consumers:
                if consumer.get("queue", {}).get("name") == queue_name:
                    consumer_tag = consumer.get("consumer_tag")
                    logger.info(f"Consumer Tag: {consumer_tag}")
                    
                    worker_pid = None
                    if '_' in consumer_tag:
                        try:
                            worker_pid = int(consumer_tag.split('_')[1])
                        except (IndexError, ValueError):
                            logger.error(f"Could not extract PID from consumer tag: {consumer_tag}")
                    
                    # Get log file links using LogRoutes class
                    _, static_url, full_url = LogRoutes.get_log_file_link(worker_pid)
                    
                    worker_info = {
                        "consumer_tag": consumer_tag,
                        "channel_details": consumer.get("channel_details"),
                        "connection_details": consumer.get("connection_details", {}),
                        "log_file": {
                            "static_url": static_url,
                            "full_url": full_url,
                            "view_url": f"{request.host_url}logs/workers/view-logs/{worker_pid}",
                        },
                        "ip_address": request.remote_addr,
                        "pid": worker_pid
                    }
                    queue_workers.append(worker_info)
            
            return jsonify({
                "queue_name": queue_name,
                "worker_count": len(queue_workers),
                "workers": queue_workers
            }), 200
        except Exception as e:
            logger.error(f"Error in find_queue_workers: {str(e)}")
            return jsonify({"error": str(e)}), 500
