from .base import BaseWorker
from app.config.logger import LoggerSetup
import json
import requests
from flask import request, jsonify
import sys

class Worker2(BaseWorker):
    def __init__(self, channel, queue_name):
        # Set worker name first
        self.worker_name = 'worker2'
        
        # Call the parent class's __init__ after setting worker_name
        super().__init__(channel, queue_name)
        
        # Set up logger for Worker2
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(worker_name=self.worker_name, worker_type="worker2")
        self.logger.info("Worker2 initialized and ready to process messages")

    def process_message(self, ch, method, properties, body):
        try:
            # Process the message
            data = json.loads(body)
            self.logger.info("Worker2 starting message processing")
            self.logger.info(f"Processing message in Worker2: {data}")
            
            # Your processing logic here
            self.logger.info("Worker2 performing message processing...")
            
            # Log successful completion
            self.logger.info("Worker2 successfully processed message")
            self.logger.info(f"Message processing completed successfully in Worker2: {data}")
            
            # Explicitly acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
            
        except Exception as e:
            self.logger.error(f"Worker2 error processing message: {str(e)}")
            self.logger.error(f"Failed message in Worker2: {body}")
            # Negative acknowledge the message and requeue it
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return False

    def publish_message(self, message_data):
        # Start the first worker in sequence
        first_worker = self.worker_sequence[0]
        try:
            # Start first worker without scaling down existing ones
            worker_data = {
                "count": 1,
                "worker_name": first_worker
            }
            # Correctly format the worker URL
            worker_url = f"http://{request.host}/worker/scale/{self.queue_name}"
            worker_response = requests.post(worker_url, json=worker_data)
            
            if worker_response.status_code != 200:
                self.logger.error(f"Failed to start worker: {worker_response.text}")
                return jsonify({
                    "message": "Message published but worker start failed",
                    "queue": self.queue_name,
                    "message_id": message_data.get("message_id"),
                    "worker_error": worker_response.text
                }), 200
        except Exception as e:
            self.logger.error(f"Failed to publish message: {str(e)}")
            return jsonify({
                "message": "Message publishing failed",
                "queue": self.queue_name,
                "message_id": message_data.get("message_id"),
                "error": str(e)
            }), 500
