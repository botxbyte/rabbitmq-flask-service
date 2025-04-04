from .base import BaseWorker
from app.config.logger import LoggerSetup
import os
import json

class Worker1(BaseWorker):
    def __init__(self, channel, queue_name):
        # Call the parent class's __init__ which already sets up the logger
        super().__init__(channel, queue_name)
        
        # No need to create another logger instance since BaseWorker already does it
        # Just bind additional context to the existing logger
        self.logger = self.logger.bind(worker_name="worker1")
        self.worker_name = 'worker1'

    def process_message(self, ch, method, properties, body):
        try:
            # Process the message
            data = json.loads(body)  # Assuming the body is a JSON string
            self.logger.info(f"Processing message: {data}")
            # Your processing logic here
            # Acknowledge the message if processed successfully
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            # Reject the message if processing fails
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return False