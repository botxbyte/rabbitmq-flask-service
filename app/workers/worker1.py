from .base import BaseWorker
from app.config.logger import LoggerSetup
import os
import json

class Worker1(BaseWorker):
    def __init__(self, channel, queue_name):
        # Set worker name first
        self.worker_name = 'worker1'
        
        # Call the parent class's __init__ after setting worker_name
        super().__init__(channel, queue_name)
        
        # Set up logger for Worker1
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(worker_name=self.worker_name, worker_type="worker1")
        self.logger.info("Worker1 initialized and ready to process messages")

    def process_message(self, ch, method, properties, body):
        try:
            # Process the message
            data = json.loads(body)  # Parse the JSON body
            self.logger.info(f"Processing message: {data}")
            
            # Your processing logic here
            
            # Log successful completion
            self.logger.info(f"Message processing completed successfully for data: {data}")
            
            # Explicitly acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        except Exception as e:
            self.logger.error(f"Message processing failed for data: {body}. Error: {str(e)}")
            # Negative acknowledge the message and requeue it
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return False