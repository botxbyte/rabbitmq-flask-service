from abc import ABC, abstractmethod
import threading
import uuid
import os
import json
from app.config.logger import LoggerSetup
import requests
from app.config.config import RABBITMQ_HOST, RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_API_PORT
import pika
import time
from datetime import datetime

class BaseWorker(ABC):
    def __init__(self, channel, queue_name):
        self.channel = channel
        self.queue_name = queue_name
        self.pid = os.getpid()
        self.worker_name = None  # Will be set by child classes
        self.start_time = datetime.now().isoformat()
        self.worker_id = f"{os.getpid()}_{int(time.time())}"  # Store worker_id directly
        
        # Create an instance of LoggerSetup
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        # Bind base worker context
        self.logger = self.logger.bind(
            worker_type="base_worker", 
            queue=queue_name,
            worker_id=self.worker_id
        )
        
        self.consumer_tag = None
        self.thread = None
        self.is_running = True

    def setup_worker_queue(self):
        """Set up worker queue after worker_name is set"""
        if not self.worker_name:
            raise ValueError("Worker name must be set before setting up queue")
            
        self.logger.info(f"Setting up worker queue: {self.queue_name}")
        
        try:
            # Create our exchange
            self.exchange_name = f"{self.queue_name}_exchange"
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='direct',
                durable=True
            )
            
            # Declare the queue (won't create if it exists)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            
            # Bind to our exchange
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=self.queue_name
            )
            
            self.logger.info(f"Successfully set up worker queue: {self.queue_name}")
        except Exception as e:
            self.logger.error(f"Failed to set up worker queue: {str(e)}")
            raise

    def forward_to_worker(self, next_worker, next_queue, data, properties):
        """Forward message to next worker in sequence"""
        try:
            next_exchange = f"{next_queue}_exchange"
            
            self.logger.info(f"Forwarding message to {next_worker} via queue {next_queue}")
            
            # Create a new channel for publishing
            publish_channel = self.channel.connection.channel()
            try:
                # Declare the exchange for the next worker's queue
                publish_channel.exchange_declare(
                    exchange=next_exchange,
                    exchange_type='direct',
                    durable=True
                )
                
                # Ensure next worker's queue exists
                publish_channel.queue_declare(queue=next_queue, durable=True)
                
                # Bind the queue to the exchange
                publish_channel.queue_bind(
                    exchange=next_exchange,
                    queue=next_queue,
                    routing_key=next_queue
                )
                
                # Add source worker info to message
                data['source'] = {
                    'worker_name': self.worker_name,
                    'queue': self.queue_name,
                    'worker_id': self.worker_id  # Use stored worker_id
                }
                
                # Publish the message
                publish_channel.basic_publish(
                    exchange=next_exchange,
                    routing_key=next_queue,
                    body=json.dumps(data),
                    properties=properties
                )
                self.logger.info(f"Successfully forwarded message to {next_worker}")
                return True
            finally:
                publish_channel.close()
        except Exception as e:
            self.logger.error(f"Error forwarding message to {next_worker}: {str(e)}")
            return False

    def _consume_wrapper(self):
        """Wrapper method to handle channel start_consuming in a thread"""
        try:
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Base Worker: Error in consumer thread: {str(e)}")
        finally:
            self.logger.info("Base Worker: Consumer thread stopping")
            
    def start(self):
        """Start the worker"""
        if not self.worker_name:
            raise ValueError("Worker name must be set before starting")
            
        def callback(ch, method, properties, body):
            try:
                # Process the message
                result = self.process_message(ch, method, properties, body)
                
                # Note: We don't need to acknowledge here since it's handled in process_message
                if not result:
                    self.logger.error("Message processing failed")
            except Exception as e:
                self.logger.error(f"Error processing message: {str(e)}")
                # Negative acknowledge the message and requeue it
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # Add a delay before setting up the consumer
        time.sleep(1)  # Wait for 1 second before setting up consumer

        # Set up basic consume
        self.consumer_tag = f"{self.worker_name}_{self.pid}"
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            consumer_tag=self.consumer_tag,
            on_message_callback=callback,
            auto_ack=False  # Make sure auto_ack is False
        )

        self.thread = threading.Thread(
            target=self._consume_wrapper,
            daemon=True,
            name=f"consumer-{self.queue_name}-{self.worker_name}"
        )
        self.thread.start()
        self.logger.info(f"Started {self.worker_name} on queue: {self.queue_name}")
        return self

    def stop(self):
        """Stop the worker gracefully"""
        self.is_running = False
        if self.consumer_tag:
            try:
                self.channel.basic_cancel(self.consumer_tag)
            except Exception as e:
                self.logger.error(f"Error cancelling consumer: {str(e)}")
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        try:
            self.channel.close()
        except Exception as e:
            self.logger.error(f"Error closing channel: {str(e)}")

    def is_alive(self):
        """Check if worker is still running"""
        return self.is_running and self.thread and self.thread.is_alive()

    @abstractmethod
    def process_message(self, ch, method, properties, body):
        """Process a message from the queue
        
        :param pika.channel.Channel ch: The channel object
        :param pika.spec.Basic.Deliver method: The Basic.Deliver method
        :param pika.spec.BasicProperties properties: The message properties
        :param bytes body: The message body
        :returns: True if processing was successful, False otherwise
        :rtype: bool
        """
        pass