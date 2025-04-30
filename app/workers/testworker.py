from .base import BaseWorker
from app.config.logger import LoggerSetup
import json
import time

class TestWorker(BaseWorker):
    WORKER_NAME = "testworker"
    def __init__(self, channel, queue_name):
        # Set worker name first
        self.worker_name = self.WORKER_NAME
        
        # Call parent class's __init__
        super().__init__(channel, queue_name)
        
        # Set up logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(worker_name=self.worker_name, worker_type="testworker")
        self.logger.info("TestWorker initialized")

    def process_message(self, ch, method, properties, body):
        """Process a message from the queue
        
        Args:
            ch: The channel object
            method: The delivery method
            properties: Message properties
            body: The message body
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Log start of processing
            self.logger.info("Starting message processing")
            
            # Parse message data
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in message: {str(e)}")
                return False
                
            # Log message details
            self.logger.info(f"Processing message: {data}")
            
            # Get message configuration
            message_config = data.get('config', {})
            process_time = message_config.get('process_time', 1)  # Default 1 second
            should_fail = message_config.get('should_fail', False)
            
            # Simulate processing
            time.sleep(process_time)
            
            # Check if we should simulate failure
            if should_fail:
                self.logger.warning("Simulating message processing failure")
                return False
                
            # Log successful completion
            self.logger.info("Message processed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            return False