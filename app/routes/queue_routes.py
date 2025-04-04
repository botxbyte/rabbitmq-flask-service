from flask import Blueprint, request, jsonify
import pika
import requests
from datetime import datetime
import uuid
import json
from app.rabbitmq import get_rabbitmq_connection
from app.config.logger import LoggerSetup
from app.config.config import RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT

queue_bp = Blueprint('queue', __name__)

# Create an instance of LoggerSetup
logger_setup = LoggerSetup()

# Use the setup_logger method to get the logger
logger = logger_setup.setup_logger()
RABBITMQ_AUTH = (RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

class QueueRoutes:
    @staticmethod
    @queue_bp.route("/list", methods=["GET"])
    def list_queues():
        try:
            # Get queues
            queues_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/queues"
            print(queues_url, 'queues_url')
            queues_response = requests.get(queues_url, auth=RABBITMQ_AUTH)
            
            if queues_response.status_code != 200:
                return jsonify({"error": "Failed to fetch queues"}), queues_response.status_code
            
            # Get consumers/workers
            consumers_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/consumers"
            consumers_response = requests.get(consumers_url, auth=RABBITMQ_AUTH)
            
            if consumers_response.status_code != 200:
                return jsonify({"error": "Failed to fetch workers"}), consumers_response.status_code
            
            queues = queues_response.json()
            consumers = consumers_response.json()
            
            queue_details = []
            for queue in queues:
                queue_workers = [
                    {
                        "consumer_tag": consumer.get("consumer_tag"),
                        "channel_details": consumer("channel_details"),
                        "connection_details": consumer.get("connection_details", {}),
                        "pid": consumer.get("channel_details", {}).get("peer_port")
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
                    "worker_pids": [w["pid"] for w in queue_workers]
                })

            return jsonify({
                "status": "connected",
                "queue_count": len(queue_details),
                "queues": queue_details
            }), 200
            
        except Exception as e:
            logger.error(f"Error in list_queues: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @staticmethod
    @queue_bp.route("/create", methods=["POST"])
    def create_queue():
        try:
            data = request.json
            queue_name = data.get("queue_name")
            if not queue_name:
                return jsonify({"error": "queue_name is required"}), 400
            
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            connection.close()
            
            logger.info(f"Queue '{queue_name}' created successfully")
            return jsonify({"message": f"Queue '{queue_name}' created successfully"}), 201
        except Exception as e:
            logger.error(f"Error in create_queue: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @staticmethod
    @queue_bp.route("/delete/<queue_name>", methods=["DELETE"])
    def delete_queue(queue_name):
        try:
            # Define vhost and encode it
            vhost = "/"  # Your RabbitMQ virtual host
            encoded_vhost = "%2F"  # URL-encoded version of '/'

            # Build URL to list all queues in the vhost
            queues_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/queues/{encoded_vhost}"
            queues_response = requests.get(queues_url, auth=RABBITMQ_AUTH)

            if queues_response.status_code != 200:
                logger.error(f"Failed to fetch queues: {queues_response.text}")
                return jsonify({"error": "Failed to fetch queues"}), queues_response.status_code

            queues = queues_response.json()
            queue_exists = any(queue["name"] == queue_name for queue in queues)

            if not queue_exists:
                return jsonify({"error": f"Queue '{queue_name}' not found"}), 404

            # Delete the queue
            delete_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/queues/{encoded_vhost}/{queue_name}"
            delete_response = requests.delete(delete_url, auth=RABBITMQ_AUTH)

            logger.info(f"Delete response status: {delete_response.status_code}, content: {delete_response.text}")

            if delete_response.status_code != 204:
                return jsonify({
                    "error": "Failed to delete queue",
                    "details": delete_response.text
                }), delete_response.status_code

            logger.success(f"Queue '{queue_name}' deleted successfully")
            return jsonify({"message": f"Queue '{queue_name}' deleted successfully"}), 200

        except Exception as e:
            logger.exception(f"Error deleting queue {queue_name}")
            return jsonify({"error": str(e)}), 500
    
    @staticmethod
    @queue_bp.route("/publish/<queue_name>", methods=["POST"])
    def publish_message(queue_name):
        try:
            data = request.json
            
            if not data or (isinstance(data, dict) and not data):
                return jsonify({"error": "Message content cannot be empty"}), 400

            # Check if the queue exists
            queues_url = f"http://{RABBITMQ_HOST}:{RABBITMQ_API_PORT}/api/queues"
            queues_response = requests.get(queues_url, auth=RABBITMQ_AUTH)
            
            if queues_response.status_code != 200:
                return jsonify({"error": "Failed to fetch queues"}), queues_response.status_code
            
            queues = queues_response.json()
            queue_exists = any(queue["name"] == queue_name for queue in queues)

            if not queue_exists:
                return jsonify({"error": f"Queue '{queue_name}' not found"}), 404

            # Publish the message
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            message_data = {
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now().isoformat(),
                **data
            }
            
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            
            connection.close()
            logger.info(f"Published message to {queue_name}: {message_data['message_id']}")
            
            return jsonify({
                "message": "Message published successfully",
                "queue": queue_name,
                "message_id": message_data["message_id"]
            }), 200
            
        except Exception as e:
            logger.error(f"Error in publish_task: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @staticmethod
    @queue_bp.route("/clear/<queue_name>", methods=["POST"])
    def clear_queue(queue_name):
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            queue_info = channel.queue_declare(queue=queue_name, passive=True)
            message_count = queue_info.method.message_count
            
            channel.queue_purge(queue=queue_name)
            connection.close()
            
            logger.info(f"Cleared queue {queue_name}: removed {message_count} messages")
            
            return jsonify({
                "message": "Queue cleared successfully",
                "queue_name": queue_name,
                "messages_removed": message_count
            }), 200
            
        except Exception as e:
            logger.error(f"Error clearing queue {queue_name}: {str(e)}")
            return jsonify({"error": str(e)}), 500

