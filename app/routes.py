# app/routes.py
import json
from flask import Blueprint, request, jsonify
import pika
from app.rabbitmq import get_rabbitmq_connection
import os
import requests

import uuid
from datetime import datetime
import time
from app.config.logger import setup_worker_logger
import multiprocessing
import importlib
import inspect
from pathlib import Path
from app.workers.base import BaseWorker
from flask import Flask, send_from_directory

STATIC_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

queue_bp = Blueprint('queue', __name__)
app = Flask(__name__, static_folder=STATIC_FOLDER)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")


LOG_BASE_DIR = "logs/workers"
# Add logger for routes
logger = setup_worker_logger("api")

# Add this at the top with other imports
worker_processes = {}  # Global dict to track worker processes



@app.route('/static/<path:filename>')
def custom_static(filename):
    return send_from_directory(STATIC_FOLDER, filename)


def get_available_workers():
    """Dynamically load all worker classes from the workers directory"""
    workers = {}
    workers_dir = Path(__file__).parent / 'workers'
    
    # Get all .py files in workers directory
    worker_files = [f for f in os.listdir(workers_dir) if f.endswith('.py') and f != '__init__.py' and f != 'base.py']
    
    for file in worker_files:
        module_name = file[:-3]  # Remove .py extension
        try:
            # Import the module
            module = importlib.import_module(f'app.workers.{module_name}')
            
            # Find worker classes in the module
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and 
                    issubclass(obj, BaseWorker) and 
                    obj != BaseWorker):
                    # Use lowercase module name as worker type
                    worker_type = module_name.lower()
                    workers[worker_type] = obj
                    
        except Exception as e:
            logger.error(f"Error loading worker module {module_name}: {str(e)}")
    
    return workers

@queue_bp.route("/queue/list", methods=["GET"])
def list_queues():
    try:
        # Get queues
        queues_url = f"http://{RABBITMQ_HOST}:15672/api/queues"
        queues_response = requests.get(queues_url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if queues_response.status_code != 200:
            return jsonify({"error": "Failed to fetch queues"}), queues_response.status_code
        
        # Get consumers/workers
        consumers_url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        consumers_response = requests.get(consumers_url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if consumers_response.status_code != 200:
            return jsonify({"error": "Failed to fetch workers"}), consumers_response.status_code
        
        queues = queues_response.json()
        consumers = consumers_response.json()
        
        # Process queues and their workers
        queue_details = []
        for queue in queues:
            queue_workers = [
                {
                    "consumer_tag": consumer["consumer_tag"],
                    "channel_details": consumer["channel_details"],
                    "connection_details": consumer.get("connection_details", {}),
                    "pid": consumer.get("channel_details", {}).get("peer_port"),  # Use peer_port as PID
                    "worker_pid": os.getpid()  # Current process PID
                }
                for consumer in consumers
                if consumer["queue"]["name"] == queue["name"]
            ]
            
            queue_details.append({
                "name": queue["name"],
                "messages": queue["messages"],
                "messages_ready": queue["messages_ready"],
                "messages_unacknowledged": queue["messages_unacknowledged"],
                "worker_count": len(queue_workers),
                "workers": queue_workers,
                "worker_pids": [w["pid"] for w in queue_workers]  # List of worker PIDs
            })

        return jsonify({
            "status": "connected",
            "queue_count": len(queue_details),
            "queues": queue_details
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@queue_bp.route("/queue/create", methods=["POST"])
def create_queue():
    try:
        data = request.json
        queue_name = data.get("queue_name")
        if not queue_name:
            return jsonify({"error": "queue_name is required"}), 400
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        # Make queue durable by default
        channel.queue_declare(queue=queue_name, durable=True)
        connection.close()
        return jsonify({"message": f"Queue '{queue_name}' created successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@queue_bp.route("/queue/scale", methods=["POST"])
def scale_worker():
    try:
        data = request.json
        worker_type = data.get("worker_type")
        count = data.get("count", 1)
        queue_name = data.get("queue_name")

        if not all([worker_type, queue_name]):
            return jsonify({"error": "worker_type and queue_name are required"}), 400

        # Get available workers dynamically
        available_workers = get_available_workers()
        
        if worker_type not in available_workers:
            return jsonify({
                "error": f"Invalid worker type: {worker_type}",
                "available_types": list(available_workers.keys())
            }), 400

        worker_class = available_workers[worker_type]
        # ... rest of scaling logic ...
        return jsonify({"message": f"Scaling workers to {count}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@queue_bp.route("/queue/workers/<queue_name>", methods=["GET"])
def get_queue_workers(queue_name):
    try:
        url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch workers"}), response.status_code
        
        consumers = response.json()
        queue_workers = []
        
        for consumer in consumers:
            if consumer["queue"]["name"] == queue_name:
                # Get connection details
                connection_details = consumer.get("connection_details", {})
                channel_details = consumer.get("channel_details", {})
                
                # Get PIDs from different sources
                worker_details = {
                    "consumer_tag": consumer["consumer_tag"],
                    "channel_details": channel_details,
                    "connection_details": connection_details,
                    "process_id": channel_details.get("pid"),  # Process ID from channel
                    "connection_pid": connection_details.get("pid"),  # Connection PID
                    "peer_port": channel_details.get("peer_port"),  # Port can be used as unique identifier
                    "node": consumer.get("node"),  # RabbitMQ node name
                    "worker_status": "active"
                }
                queue_workers.append(worker_details)
        
        return jsonify({
            "queue_name": queue_name,
            "worker_count": len(queue_workers),
            "workers": queue_workers,
            "pids": {
                "process_pids": [w["process_id"] for w in queue_workers if w["process_id"]],
                "connection_pids": [w["connection_pid"] for w in queue_workers if w["connection_pid"]],
                "peer_ports": [w["peer_port"] for w in queue_workers if w["peer_port"]]
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting workers: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Function to get the log file path
def get_log_path(pid=None):
    current_date = datetime.now()
    year = str(current_date.year)
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    base_path = os.path.join("app", "static", "logs", "workers", year, month, day)
    os.makedirs(base_path, exist_ok=True)

    filename = "worker"
    if pid:
        filename += f"_{pid}"
    filename += f"_{current_date.strftime('%Y%m%d')}.log"

    return os.path.join(base_path, filename)

# Function to get the full URL and static path
def get_log_file_link(pid=None):
    log_path = get_log_path(pid)
    log_filename = os.path.basename(log_path)

    # Full path of the log file
    full_path = os.path.abspath(log_path)

    # Relative static path
    relative_path = os.path.relpath(log_path, start=os.path.join("app", "static"))
    static_url = f"/static/{relative_path.replace(os.sep, '/')}"  # Static URL path

    # Build full URL dynamically
    full_url = request.host_url.rstrip('/') + static_url  # e.g., http://127.0.0.1:5000/static/...

    return full_path, static_url, full_url

# Endpoint to include full URL and IP
@queue_bp.route("/queue/workers", methods=["POST"])
def find_queue_workers():
    try:
        data = request.json
        queue_name = data.get("queue_name")
        
        if not queue_name:
            return jsonify({"error": "queue_name is required"}), 400

        url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch workers"}), response.status_code
        
        consumers = response.json()
        queue_workers = [
            {
                "consumer_tag": consumer["consumer_tag"],
                "channel_details": consumer["channel_details"],
                "connection_details": consumer.get("connection_details", {}),
                "log_file": {
                    # "full_path": get_log_file_link(consumer.get("pid"))[0],
                    "static_url": get_log_file_link(consumer.get("pid"))[1],
                    "full_url": get_log_file_link(consumer.get("pid"))[2]  # Full URL
                },
                "ip_address": request.remote_addr
            }
            for consumer in consumers
            if consumer["queue"]["name"] == queue_name
        ]
        
        return jsonify({
            "queue_name": queue_name,
            "worker_count": len(queue_workers),
            "workers": queue_workers
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
            
# @queue_bp.route('/logs/workers/<path:filename>', methods=['GET'])
# def serve_log_file(filename):
#     try:
#         return send_from_directory(os.path.join("logs", "workers"), filename, as_attachment=True)
#     except FileNotFoundError:
#         return jsonify({"error": "Log file not found"}), 404


# Global worker registry
worker_registry = {}

def start_worker(queue_name, worker_type):
    try:
        # Add current process ID to worker info
        current_pid = os.getpid()
        logger.info(f"Starting worker process {current_pid} for queue {queue_name}")
        
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        
        # Get available workers dynamically
        available_workers = get_available_workers()
        
        if worker_type not in available_workers:
            raise ValueError(f"Invalid worker type: {worker_type}")
            
        WorkerClass = available_workers[worker_type]
        worker = WorkerClass(channel, queue_name)
        
        # Ensure queue exists
        channel.queue_declare(queue=queue_name, durable=True, passive=True)
        
        def callback(ch, method, properties, body):
            try:
                worker.process_message(ch, method, properties, body)
            except Exception as e:
                logger.error(f"Error in callback: {str(e)}")
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

        # Register consumer with process ID in consumer tag
        consumer_tag = f"worker_{current_pid}"
        channel.basic_consume(
            queue=queue_name,
            consumer_tag=consumer_tag,
            on_message_callback=callback
        )
        
        channel.start_consuming()
        
    except Exception as e:
        logger.error(f"Worker process failed: {str(e)}")
    finally:
        try:
            if channel and channel.is_open:
                channel.close()
            if connection and connection.is_open:
                connection.close()
        except Exception:
            pass

@queue_bp.route("/queue/scale/<queue_name>", methods=["POST"])
def scale_queue_workers(queue_name):
    try:
        data = request.json
        desired_count = int(data.get("count", 0))
        worker_type = data.get("worker_type")

        # Get available workers dynamically
        available_workers = get_available_workers()
        
        if not worker_type or worker_type not in available_workers:
            return jsonify({
                "error": "Invalid worker type",
                "available_types": list(available_workers.keys())
            }), 400

        # Clean up dead processes
        for pid in list(worker_processes.keys()):
            if not worker_processes[pid]['process'].is_alive():
                logger.info(f"Removing dead process {pid}")
                del worker_processes[pid]

        # Get current workers
        url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch workers"}), response.status_code

        current_workers = [
            w for w in response.json() 
            if w["queue"]["name"] == queue_name
        ]
        current_count = len(current_workers)

        # ... rest of the existing scaling logic ...
        # Scale down if needed  
        if current_count > desired_count:
            for worker in current_workers[:current_count - desired_count]:
                try:
                    consumer_tag = worker["consumer_tag"]
                    pid = int(consumer_tag.split('_')[1]) if '_' in consumer_tag else None
                    
                    if pid and pid in worker_processes:
                        worker_processes[pid]['process'].terminate()
                        del worker_processes[pid]
                        logger.info(f"Terminated worker process {pid}")
                except Exception as e:
                    logger.error(f"Error removing worker: {str(e)}")

        # Scale up if needed
        if current_count < desired_count:
            for _ in range(desired_count - current_count):
                try:
                    process = multiprocessing.Process(
                        target=start_worker,
                        args=(queue_name, worker_type)
                    )
                    process.daemon = True
                    process.start()
                    
                    worker_processes[process.pid] = {
                        'process': process,
                        'queue': queue_name,
                        'type': worker_type
                    }
                    logger.info(f"Started worker process {process.pid}")
                except Exception as e:
                    logger.error(f"Error starting worker: {str(e)}")

        # Wait for workers to stabilize
        time.sleep(5)

        # Get final state
        verify_response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        final_workers = []

        if verify_response.status_code == 200:
            consumers = verify_response.json()
            for w in consumers:
                if w["queue"]["name"] == queue_name:
                    consumer_tag = w["consumer_tag"]
                    pid = int(consumer_tag.split('_')[1]) if '_' in consumer_tag else None
                    
                    if pid and pid in worker_processes:
                        final_workers.append({
                            "consumer_tag": consumer_tag,
                            "channel": w["channel_details"]["name"],
                            "connection": w.get("connection_details", {}).get("name", ""),
                            "pid": pid,
                            "worker_status": "active"
                        })

        final_count = len(final_workers)
        success = final_count == desired_count

        # Get active PIDs
        active_pids = [w["pid"] for w in final_workers]

        return jsonify({
            "queue_name": queue_name,
            "worker_type": worker_type,
            "previous_count": current_count,
            "current_count": final_count,
            "target_count": desired_count,
            "workers": final_workers,
            "process_pids": active_pids,
            "workers_added": max(0, final_count - current_count),
            "workers_removed": max(0, current_count - final_count),
            "status": "scaled" if success else "partial_scale",
            "success": success
        }), 200

    except Exception as e:
        logger.error(f"Error scaling workers: {str(e)}")
        return jsonify({
            "error": str(e),
            "queue_name": queue_name,
            "workers_added": 0,
            "status": "failed"
        }), 500
    
@queue_bp.route("/queue/publish/<queue_name>", methods=["POST"])
def publish_task(queue_name):
    try:
        data = request.json
        worker_type = data.get("worker_type")
        domain_name = data.get("domain_name")
        
        # Get available workers dynamically
        available_workers = get_available_workers()
        
        if not worker_type or worker_type not in available_workers:
            return jsonify({
                "error": "Invalid worker type",
                "valid_types": list(available_workers.keys())
            }), 400
            
        if not domain_name:
            return jsonify({"error": "domain_name is required"}), 400

        # Check for active workers of the correct type
        url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to check worker status"}), response.status_code
            
        # Get workers and their types from registry
        matching_workers = []
        for worker in response.json():
            if worker["queue"]["name"] == queue_name:
                consumer_tag = worker["consumer_tag"]
                pid = int(consumer_tag.split('_')[1]) if '_' in consumer_tag else None
                
                # Check if worker exists in registry and matches type
                if pid in worker_processes and worker_processes[pid]["type"] == worker_type:
                    matching_workers.append(worker)
                    
        logger.info(f"Queue: {queue_name}, Worker type: {worker_type}")
        logger.info(f"Found {len(matching_workers)} matching workers")
        
        if not matching_workers:
            return jsonify({
                "error": f"No active workers found for type: {worker_type}", 
                "message": f"Please scale workers of type '{worker_type}' for this queue first",
                "current_workers": [
                    {
                        "consumer_tag": w["consumer_tag"],
                        "queue": w["queue"]["name"],
                        "pid": w["channel_details"].get("peer_port"),
                        "type": worker_processes.get(int(w["consumer_tag"].split('_')[1]), {}).get("type", "unknown")
                    }
                    for w in response.json()
                    if w["queue"]["name"] == queue_name
                ],
                "worker_processes": list(worker_processes.keys())
            }), 400

        # Publish the message with worker type
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        message_data = {
            "worker_type": worker_type,
            "domain": domain_name,
            "task_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat()
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message_data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json',
                headers={'worker_type': worker_type}  # Add worker type in headers
            )
        )
        
        connection.close()
        logger.info(f"Published {worker_type} task to {queue_name}: {message_data['task_id']}")
        
        return jsonify({
            "message": "Task published successfully",
            "queue": queue_name,
            "worker_type": worker_type,
            "domain": domain_name,
            "task_id": message_data["task_id"],
            "matching_workers": len(matching_workers)
        }), 200
        
    except Exception as e:
        logger.error(f"Error in publish_task: {str(e)}")
        return jsonify({"error": str(e)}), 500

@queue_bp.route("/queue/clear/<queue_name>", methods=["POST"])
def clear_queue(queue_name):
    try:
        # Stop all workers first
        url = f"http://{RABBITMQ_HOST}:15672/api/consumers"
        response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch workers"}), response.status_code
            
        # Get workers for this queue
        queue_workers = [
            worker for worker in response.json() 
            if worker["queue"]["name"] == queue_name
        ]
        
        # Stop each worker
        workers_stopped = 0
        for worker in queue_workers:
            channel_id = worker["channel_details"]["name"]
            consumer_tag = worker["consumer_tag"]
            stop_url = f"http://{RABBITMQ_HOST}:15672/api/consumers/{channel_id}/{consumer_tag}"
            delete_response = requests.delete(stop_url, auth=(RABBITMQ_USER, RABBITMQ_PASS))
            if delete_response.status_code == 204:
                workers_stopped += 1
                logger.info(f"Stopped worker {consumer_tag} for queue {queue_name}")
        
        # Now purge the queue
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Get message count before purge
        queue_info = channel.queue_declare(queue=queue_name, passive=True)
        message_count = queue_info.method.message_count
        
        # Purge the queue
        channel.queue_purge(queue=queue_name)
        
        connection.close()
        
        logger.info(f"Cleared queue {queue_name}: removed {message_count} messages and stopped {workers_stopped} workers")
        
        return jsonify({
            "message": "Queue cleared successfully",
            "queue_name": queue_name,
            "messages_removed": message_count,
            "workers_stopped": workers_stopped
        }), 200
        
    except Exception as e:
        logger.error(f"Error clearing queue {queue_name}: {str(e)}")
        return jsonify({"error": str(e)}), 500