from flask import Blueprint, request, jsonify
import multiprocessing
import time
import requests
import os
from pathlib import Path
import importlib
import inspect
from app.workers.base import BaseWorker
from app.rabbitmq import get_rabbitmq_connection
from app.config.logger import LoggerSetup
from app.config.config import RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT
from app.routes.log_routes import LogRoutes

worker_bp = Blueprint('worker', __name__)

# Create an instance of LoggerSetup
logger_setup = LoggerSetup()

# Use setup_logger for route files
logger = logger_setup.setup_logger()
RABBITMQ_AUTH = (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

# Global worker registry
worker_processes = {}

class WorkerRoutes:
    @staticmethod
    def get_available_workers():
        """Dynamically load all worker classes from the workers directory"""
        workers = {}
        workers_dir = Path(__file__).parent.parent / 'workers'
        
        worker_files = [f for f in os.listdir(workers_dir) if f.endswith('.py') and f != '__init__.py' and f != 'base.py']
        
        for file in worker_files:
            module_name = os.path.splitext(file)[0]  # Using splitext instead of slicing
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
            current_pid = os.getpid()
            logger.info(f"Starting worker process {current_pid} for queue {queue_name}")
            
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            
            available_workers = WorkerRoutes.get_available_workers()
            
            if worker_name not in available_workers:
                raise ValueError(f"Invalid worker name: {worker_name}")
                
            WorkerClass = available_workers[worker_name]
            worker = WorkerClass(channel, queue_name)
            
            channel.queue_declare(queue=queue_name, durable=True, passive=True)
            
            def callback(ch, method, properties, body):
                try:
                    worker.process_message(ch, method, properties, body)
                except Exception as e:
                    logger.error(f"Error in callback: {str(e)}")
                    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

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

    @staticmethod
    @worker_bp.route("/workers/<queue_name>", methods=["GET"])
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
    @worker_bp.route("/scale/<queue_name>", methods=["POST"])
    def scale_queue_workers(queue_name):
        try:
            data = request.json
            desired_count = int(data.get("count", 0))
            worker_name = data.get("worker_name")

            available_workers = WorkerRoutes.get_available_workers()
            
            if not worker_name or worker_name not in available_workers:
                return jsonify({
                    "error": "Invalid worker name",
                    "available_names": list(available_workers.keys())
                }), 400

            # Clean up dead processes
            for pid in list(worker_processes.keys()):
                if not worker_processes[pid]['process'].is_alive():
                    logger.info(f"Removing dead process {pid}")
                    del worker_processes[pid]

            # Get current workers
            url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            response = requests.get(url, auth=RABBITMQ_AUTH)
            
            if response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), response.status_code

            current_workers = [
                w for w in response.json() 
                if w.get("queue", {}).get("name") == queue_name
            ]
            current_count = len(current_workers)

            # Scale down if needed  
            if current_count > desired_count:
                for worker in current_workers[:current_count - desired_count]:
                    try:
                        consumer_tag = worker.get("consumer_tag")
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
                            target=WorkerRoutes.start_worker,
                            args=(queue_name, worker_name)
                        )
                        process.daemon = True
                        process.start()
                        
                        worker_processes[process.pid] = {
                            'process': process,
                            'queue': queue_name,
                            'name': worker_name
                        }
                        logger.info(f"Started worker process {process.pid}")
                    except Exception as e:
                        logger.error(f"Error starting worker: {str(e)}")

            # Wait for workers to stabilize
            time.sleep(2)

            # Get final state
            verify_response = requests.get(url, auth=RABBITMQ_AUTH)
            final_workers = []

            if verify_response.status_code == 200:
                consumers = verify_response.json()
                for w in consumers:
                    if w.get("queue", {}).get("name") == queue_name:
                        consumer_tag = w.get("consumer_tag")
                        pid = int(consumer_tag.split('_')[1]) if '_' in consumer_tag else None
                        
                        if pid and pid in worker_processes:
                            final_workers.append({
                                "consumer_tag": consumer_tag,
                                "channel": w.get("channel_details", {}).get("name"),
                                "connection": w.get("connection_details", {}).get("name", ""),
                                "pid": pid,
                                "worker_status": "active"
                            })

            final_count = len(final_workers)
            success = final_count == desired_count

            return jsonify({
                "queue_name": queue_name,
                "worker_name": worker_name,
                "previous_count": current_count,
                "current_count": final_count,
                "target_count": desired_count,
                "workers": final_workers,
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

    @staticmethod
    @worker_bp.route("/workers", methods=["POST"])
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
                            "view_url": f"{request.host_url}queue/workers/view-logs/{worker_pid}",
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
