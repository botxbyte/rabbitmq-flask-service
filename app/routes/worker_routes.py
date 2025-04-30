from flask import Blueprint, request, jsonify
import multiprocessing
import time
import requests
import os
from pathlib import Path
import importlib
import inspect
import threading
import signal
from app.workers.base import BaseWorker
from app.rabbitmq import get_rabbitmq_connection
from app.config.logger import LoggerSetup
from app.config.config import RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT
from app.routes.log_routes import LogRoutes

# Create an instance of LoggerSetup
logger_setup = LoggerSetup()
logger = logger_setup.setup_logger()
RABBITMQ_AUTH = (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

# Global worker process registry with lock
worker_processes_lock = threading.Lock()
worker_processes = {}

# Create blueprint with url_prefix
worker_bp = Blueprint('worker', __name__)

def cleanup_worker_process(pid):
    """Helper function to clean up a worker process."""
    try:
        # 1) Try graceful shutdown with SIGTERM
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)  # give it a moment to exit
        except ProcessLookupError:
            # already gone
            pass
        except Exception as e:
            logger.warning(f"Could not send SIGTERM to {pid}: {e}")

        # 2) If SIGKILL exists (Unix), force‐kill remaining processes
        if hasattr(signal, 'SIGKILL'):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # already exited
            except Exception as e:
                logger.warning(f"Could not send SIGKILL to {pid}: {e}")

        # 3) Finally, remove from our registry
        with worker_processes_lock:
            if pid in worker_processes:
                del worker_processes[pid]
                logger.info(f"Removed worker process {pid} from registry")

    except Exception as e:
        logger.error(f"Error cleaning up worker process {pid}: {e}")
class WorkerRoutes:
    """Worker management routes"""
    
    @staticmethod
    def get_available_workers():
        workers = {}
        workers_dir = Path(__file__).parent.parent / 'workers'

        for fname in os.listdir(workers_dir):
            if not fname.endswith('.py') or fname in ('__init__.py', 'base.py'):
                continue

            module_name = fname[:-3]
            try:
                module = importlib.import_module(f'app.workers.{module_name}')
            except Exception as e:
                logger.error(f"Could not import worker module {module_name}: {e}")
                continue

            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, BaseWorker) and cls is not BaseWorker:
                    worker_name = getattr(cls, 'WORKER_NAME', module_name)
                    worker_key = worker_name.lower()
                    workers[worker_key] = cls
                    
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
            
            worker_class = available_workers.get(worker_name.lower())
            if not worker_class:
                error_msg = f"Invalid worker name: {worker_name}. Available workers: {list(available_workers.keys())}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"Found worker class: {worker_class.__name__}")
            
            # Create worker instance
            worker = None
            try:
                worker = worker_class(channel, queue_name)
                logger.info("Worker instance created")
                
                # Get process ID
                process_id = os.getpid()
                
                # Register worker in global registry
                with worker_processes_lock:
                    worker_processes[process_id] = {
                        'worker': worker,
                        'queue_name': queue_name,
                        'worker_name': worker_name,
                        'start_time': time.time(),
                        'status': 'starting'
                    }
                logger.info(f"Registered worker process {process_id} in registry")
                
                # Start the worker
                worker.start()
                logger.info(f"Started {worker_name} with PID: {process_id}, queue: {queue_name}")
                
                # Mark worker as started in registry
                with worker_processes_lock:
                    if process_id in worker_processes:
                        worker_processes[process_id]['status'] = 'running'
                
                # Verify worker is registered with RabbitMQ
                url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
                verify_attempts = 3
                success = False
                
                for attempt in range(verify_attempts):
                    try:
                        response = requests.get(url, auth=RABBITMQ_AUTH)
                        if response.status_code == 200:
                            consumers = response.json()
                            for consumer in consumers:
                                consumer_tag = consumer.get("consumer_tag", "")
                                queue = consumer.get("queue", {}).get("name", "")
                                if consumer_tag == f"{worker_name}_{process_id}" and queue == queue_name:
                                    success = True
                                    break
                        if success:
                            break
                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"Error verifying worker registration (attempt {attempt + 1}): {str(e)}")
                        if attempt < verify_attempts - 1:
                            time.sleep(2)
                
                if not success:
                    error_msg = f"Worker {worker_name} failed to register with RabbitMQ"
                    logger.error(error_msg)
                    # Mark worker as failed in registry before cleanup
                    with worker_processes_lock:
                        if process_id in worker_processes:
                            worker_processes[process_id]['status'] = 'failed'
                    # Cleanup the worker
                    if worker:
                        worker.stop()
                    cleanup_worker_process(process_id)
                    raise RuntimeError(error_msg)
                
                # Keep the process running and monitor the worker
                while True:
                    time.sleep(1)
                    if not worker.is_alive():
                        error_msg = f"Worker {worker_name} stopped unexpectedly"
                        logger.error(error_msg)
                        # Mark worker as failed in registry
                        with worker_processes_lock:
                            if process_id in worker_processes:
                                worker_processes[process_id]['status'] = 'failed'
                        cleanup_worker_process(process_id)
                        raise Exception(error_msg)
                
            except Exception as inner_e:
                logger.error(f"Error in worker initialization/startup: {str(inner_e)}")
                # Ensure worker is stopped if it was created
                if worker:
                    try:
                        worker.stop()
                    except Exception as stop_e:
                        logger.error(f"Error stopping worker: {str(stop_e)}")
                # Clean up process from registry
                if 'process_id' in locals():
                    cleanup_worker_process(process_id)
                raise inner_e
            
        except Exception as e:
            logger.error(f"Error starting worker: {str(e)}")
            # Ensure any started resources are cleaned up
            if 'worker' in locals() and worker:
                try:
                    worker.stop()
                except:
                    pass
            if 'process_id' in locals():
                cleanup_worker_process(process_id)
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
            except Exception:
                return jsonify({"error": f"Queue '{queue_name}' does not exist"}), 404

            # Fetch current consumers from RabbitMQ API
            max_retries = 3
            retry_delay = 2
            url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            response = None
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, auth=RABBITMQ_AUTH)
                    if response.status_code == 200:
                        break
                except Exception:
                    pass
                time.sleep(retry_delay)

            if not response or response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), response.status_code if response else 500

            # Identify current workers for this queue and worker_name
            all_consumers = response.json()
            current_workers = []
            for consumer in all_consumers:
                consumer_tag = consumer.get("consumer_tag", "")
                queue = consumer.get("queue", {}).get("name", "")
                # match tags like "<worker_name>_<pid>"
                if consumer_tag.lower().startswith(f"{worker_name.lower()}_") and queue == queue_name:
                    current_workers.append(consumer)

            current_count = len(current_workers)
            workers_removed = 0

            # Scale down
            if current_count > desired_count:
                for consumer in current_workers[: current_count - desired_count]:
                    try:
                        tag = consumer.get("consumer_tag")
                        pid = int(tag.rsplit('_', 1)[1]) if '_' in tag else None
                        if pid:
                            cleanup_worker_process(pid)
                            workers_removed += 1
                    except Exception:
                        pass
                time.sleep(3)

            # Scale up
            workers_added = 0
            if current_count < desired_count:
                for _ in range(desired_count - current_count):
                    proc = multiprocessing.Process(
                        target=WorkerRoutes.start_worker,
                        args=(queue_name, worker_name)
                    )
                    proc.daemon = False
                    proc.start()
                    time.sleep(5)
                    if proc.is_alive():
                        # verify registration
                        for _ in range(3):
                            try:
                                vr = requests.get(url, auth=RABBITMQ_AUTH)
                                if vr.status_code == 200:
                                    for c in vr.json():
                                        if c.get("consumer_tag", "").startswith(f"{worker_name}_") and c.get("queue", {}).get("name") == queue_name:
                                            workers_added += 1
                                            raise StopIteration
                            except StopIteration:
                                break
                            except Exception:
                                pass
                            time.sleep(2)
                        else:
                            cleanup_worker_process(proc.pid)
                    # no-op if not alive

            # Final fetch of workers
            time.sleep(2)
            resp_final = requests.get(url, auth=RABBITMQ_AUTH)
            final_workers = []
            if resp_final.status_code == 200:
                for consumer in resp_final.json():
                    consumer_tag = consumer.get("consumer_tag", "")
                    queue = consumer.get("queue", {}).get("name", "")
                    if consumer_tag.lower().startswith(f"{worker_name.lower()}_") and queue == queue_name:
                        try:
                            pid = int(consumer_tag.rsplit('_', 1)[1])
                        except:
                            pid = None
                        final_workers.append({
                            "consumer_tag": consumer_tag,
                            "pid": pid,
                            "worker_status": "active",
                            "queue": queue,
                            "worker_name": worker_name
                        })

            final_count = len(final_workers)
            success = (final_count == desired_count)

            # Cleanup extra if any
            if final_count > desired_count:
                for extra in final_workers[desired_count:]:
                    try:
                        cleanup_worker_process(extra['pid'])
                        workers_removed += 1
                    except Exception:
                        pass

            return jsonify({
                "queue_name": queue_name,
                "worker_name": worker_name,
                "previous_count": current_count,
                "current_count": final_count,
                "target_count": desired_count,
                "workers": final_workers,
                "workers_added": workers_added,
                "workers_removed": workers_removed,
                "status": "scaled" if success else "partial_scale",
                "success": success,
                "message": "Workers scaled successfully" if success else "Some workers failed to scale"
            }), 200

        except Exception as e:
            logger.error(f"Error scaling workers: {e}")
            return jsonify({"error": str(e)}), 500

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
                            # Extract the last part of the tag which should be the PID
                            worker_pid = int(consumer_tag.rsplit('_', 1)[1])
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
                            # Extract the last part of the tag which should be the PID
                            worker_pid = int(consumer_tag.rsplit('_', 1)[1])
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
