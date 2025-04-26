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
        self.connection = channel.connection
        self.pid = os.getpid()
        self.worker_name = 'base_worker'
        self.start_time = datetime.now().isoformat()
        self.worker_id = f"{os.getpid()}_{int(time.time())}"
        self.error_state = False  # New flag to track error state
        
        # Standard RabbitMQ connection parameters
        self.connection_params = {
            'host': RABBITMQ_HOST,
            'credentials': pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD),
            'heartbeat': 600,        # 10 minute heartbeat
            'blocked_connection_timeout': 300,  # 5 minute timeout
            'connection_attempts': 3,
            'retry_delay': 5
        }
        
        # Create logger
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(
            worker_type="base_worker", 
            queue=queue_name,
            worker_id=self.worker_id
        )
        
        self.consumer_tag = None
        self.thread = None
        self.is_running = True
        self.processing_lock = threading.Lock()

    def _consume_wrapper(self):
        """Wrapper method to handle channel start_consuming in a thread"""
        while self.is_running:
            connection = None
            channel = None
            
            try:
                # Create new connection for this consumer
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(**self.connection_params)
                )
                channel = connection.channel()
                
                try:
                    # First try to check if queue exists with passive=True
                    channel.queue_declare(
                        queue=self.queue_name, 
                        passive=True
                    )
                    self.logger.info(f"Connected to existing queue: {self.queue_name}")
                except pika.exceptions.ChannelClosedByBroker:
                    # Queue doesn't exist, create new connection and channel
                    if not connection.is_open:
                        connection = pika.BlockingConnection(
                            pika.ConnectionParameters(**self.connection_params)
                        )
                    channel = connection.channel()
                    
                    # Declare queue with minimal settings
                    channel.queue_declare(
                        queue=self.queue_name, 
                        durable=True
                    )
                    self.logger.info(f"Created new queue: {self.queue_name}")
                
                # Configure QoS - process one message at a time
                channel.basic_qos(prefetch_count=1)
                
                def callback(ch, method, properties, body):
                    if not ch.is_open:
                        self.logger.warning("Channel closed, cannot process message")
                        return
                        
                    try:
                        with self.processing_lock:
                            # Check if we're in error state
                            if self.error_state:
                                self.logger.warning("Worker in error state, not processing new messages")
                                # Requeue the message
                                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                                return
                            
                            self.logger.info(f"Processing message from queue: {self.queue_name}")
                            
                            # Start time for monitoring
                            start_time = time.time()
                            
                            # Initialize result and step_slug_id
                            result = False
                            step_slug_id = None
                            
                            try:
                                # Process the message
                                result = self.process_message(ch, method, properties, body)
                                
                                # Log processing time
                                processing_time = time.time() - start_time
                                self.logger.info(f"Task processed in {processing_time:.2f} seconds")
                                
                                # Only acknowledge if channel is still open and we have a result
                                if ch.is_open:
                                    if result:
                                        try:
                                            ch.basic_ack(delivery_tag=method.delivery_tag)
                                            self.logger.info("Message processed successfully")
                                        except (pika.exceptions.ChannelWrongStateError, 
                                                pika.exceptions.ChannelClosedByBroker) as e:
                                            self.logger.error(f"Failed to acknowledge message: {str(e)}")
                                            # Don't requeue here - let the broker handle it
                                            return
                                    else:
                                        # Handle failed processing
                                        self.logger.error("Message processing failed")
                                        retry_count = 0
                                        if properties.headers:
                                            retry_count = properties.headers.get('x-retry-count', 0)
                                        
                                        if ch.is_open:
                                            try:
                                                if retry_count >= 3:
                                                    # After 3 retries, reject the message and set error state
                                                    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                                                    self.logger.warning("Message rejected after max retries")
                                                    self.error_state = True  # Set error state
                                                else:
                                                    # Requeue with incremented retry count
                                                    headers = properties.headers or {}
                                                    headers['x-retry-count'] = retry_count + 1
                                                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                                                    self.logger.info(f"Message requeued (attempt {retry_count + 1})")
                                            except (pika.exceptions.ChannelWrongStateError,
                                                    pika.exceptions.ChannelClosedByBroker) as e:
                                                self.logger.error(f"Failed to reject/nack message: {str(e)}")
                                                return
                                        else:
                                            self.logger.warning("Channel closed during processing, message will be requeued")
                                            
                            except Exception as e:
                                self.logger.error(f"Error processing message: {str(e)}")
                                if ch.is_open:
                                    try:
                                        # Set error state and requeue message
                                        self.error_state = True
                                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                                    except (pika.exceptions.ChannelWrongStateError,
                                            pika.exceptions.ChannelClosedByBroker) as e:
                                        self.logger.error(f"Failed to nack message: {str(e)}")
                
                                # Log the message data in case of error
                                try:
                                    message_data = body.decode('utf-8')
                                    self.logger.error(f"Message processing failed for data: {message_data}")
                                except:
                                    self.logger.error("Could not decode message data")
                    
                    except Exception as e:
                        self.logger.error(f"Critical error in callback: {str(e)}")
                        if ch.is_open:
                            try:
                                # Set error state and requeue message
                                self.error_state = True
                                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            except (pika.exceptions.ChannelWrongStateError,
                                    pika.exceptions.ChannelClosedByBroker) as e:
                                self.logger.error(f"Failed to nack message after critical error: {str(e)}")
                
                # Set up consumer
                consumer_tag = f"{self.worker_name}_{self.pid}"
                channel.basic_consume(
                    queue=self.queue_name,
                    consumer_tag=consumer_tag,
                    on_message_callback=callback,
                    auto_ack=False  # Important: manual ack mode
                )
                
                self.logger.info(f"Consumer ready on queue: {self.queue_name}")
                
                # Start consuming
                while self.is_running and connection.is_open and channel.is_open:
                    try:
                        connection.process_data_events(time_limit=1)
                        
                        # If in error state, wait for a new message
                        if self.error_state:
                            self.logger.info("Worker in error state, waiting for new message...")
                            time.sleep(5)  # Wait 5 seconds before checking for new messages
                            # Reset error state to allow processing new messages
                            self.error_state = False
                            
                    except (pika.exceptions.ConnectionClosed, 
                            pika.exceptions.ChannelClosedByBroker) as e:
                        self.logger.error(f"Connection/channel closed: {str(e)}")
                        break
                    except Exception as e:
                        self.logger.error(f"Error processing events: {str(e)}")
                        break
                
            except pika.exceptions.AMQPConnectionError as e:
                self.logger.error(f"AMQP Connection error: {str(e)}")
                time.sleep(5)  # Wait before reconnecting
            except pika.exceptions.ChannelClosedByBroker as e:
                self.logger.error(f"Channel closed by broker: {str(e)}")
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Consumer thread error: {str(e)}")
                time.sleep(5)
            finally:
                # Clean up
                try:
                    if channel and not channel.is_closed:
                        channel.close()
                    if connection and not connection.is_closed:
                        connection.close()
                except Exception as e:
                    self.logger.error(f"Error closing connection: {str(e)}")
                
                if self.is_running:
                    self.logger.info("Attempting to reconnect...")
                else:
                    self.logger.info("Consumer stopping")
                    break

    def start(self):
        """Start the worker"""
        if not self.worker_name:
            raise ValueError("Worker name must be set before starting")

        # Start consumer thread
        self.thread = threading.Thread(
            target=self._consume_wrapper,
            daemon=False,
            name=f"consumer-{self.queue_name}-{self.worker_name}"
        )
        self.thread.start()

        # Monitor thread to restart consumer if it dies
        def monitor():
            while self.is_running:
                try:
                    time.sleep(30)
                    if not self.thread.is_alive():
                        self.logger.warning("Consumer thread died, restarting...")
                        self.thread = threading.Thread(
                            target=self._consume_wrapper,
                            daemon=False,
                            name=f"consumer-{self.queue_name}-{self.worker_name}"
                        )
                        self.thread.start()
                except Exception as e:
                    self.logger.error(f"Monitor error: {str(e)}")
                    
        self.monitor_thread = threading.Thread(
            target=monitor,
            daemon=True,
            name=f"monitor-{self.queue_name}"
        )
        self.monitor_thread.start()
        
        # Verify threads started
        time.sleep(0.5)
        if not all([self.thread.is_alive(), self.monitor_thread.is_alive()]):
            raise RuntimeError("Failed to start worker threads")
            
        self.logger.info(f"Started {self.worker_name} on queue: {self.queue_name}")
        return self

    def stop(self):
        """Stop the worker gracefully"""
        self.is_running = False
        
        # Stop threads
        for thread in [self.thread, self.monitor_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5)
                self.logger.info(f"Stopped {thread.name}")
        
        # Close connections
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            self.logger.error(f"Error closing connections: {str(e)}")

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