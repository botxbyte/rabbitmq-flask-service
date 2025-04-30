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
        # Initialize worker_name from class's WORKER_NAME if available
        if hasattr(self.__class__, 'WORKER_NAME'):
            self.worker_name = self.__class__.WORKER_NAME
        elif not hasattr(self, 'worker_name'):
            self.worker_name = self.__class__.__name__.lower()
            
        self.channel = channel
        self.queue_name = queue_name
        self.connection = channel.connection if channel else None
        self.pid = os.getpid()
        self.start_time = datetime.now().isoformat()
        self.worker_id = f"{os.getpid()}_{int(time.time())}"
        self.error_state = False
        
        # Updated RabbitMQ connection parameters with more robust settings
        self.connection_params = {
            'host': RABBITMQ_HOST,
            'credentials': pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD),
            'heartbeat': 300,  # 5 minutes heartbeat (good for long tasks)
            'blocked_connection_timeout': 300,  # Allow long blocking during processing
            'connection_attempts': 3,
            'retry_delay': 5,
            'socket_timeout': 300,  # Allow socket to stay open during long jobs
            'stack_timeout': 300,
            'tcp_options': {
                'TCP_KEEPIDLE': 60,
                'TCP_KEEPINTVL': 10,
                'TCP_KEEPCNT': 3
            }
        }
        # Create logger with both file and worker-specific logging
        logger_setup = LoggerSetup()
        self.logger = logger_setup.setup_worker_logger(self.pid)
        self.logger = self.logger.bind(
            worker_name=self.worker_name,
            worker_type=self.worker_name,
            queue=queue_name,
            worker_id=self.worker_id,
            pid=self.pid
        )
        
        # Log initialization
        self.logger.info(f"Initializing {self.worker_name} worker with PID {self.pid}")
        
        # For consumer tag - use consistent format
        self.consumer_tag = f"{self.worker_name}_{self.pid}"
        self.thread = None
        self.monitor_thread = None
        self.is_running = True
        self.processing_lock = threading.Lock()
        self.reconnect_delay = 5  # seconds
        self.max_retries = 5
        
        # Create initial connection
        self.connect()

    def connect(self):
        """Establish connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Create new connection
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(**self.connection_params)
                )
                self.channel = self.connection.channel()
                
                # Declare queue
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                
                # Configure QoS
                self.channel.basic_qos(prefetch_count=1)
                
                self.logger.info(f"Successfully connected on attempt {attempt + 1}")
                return True
            except Exception as e:
                self.logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.reconnect_delay)
                    continue
        return False

    def check_connection_health(self):
        """Check connection health and attempt recovery if needed"""
        try:
            if self.connection and self.connection.is_open:
                # Try to process any pending events
                self.connection.process_data_events(time_limit=0)
                return True
        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed, Exception) as e:
            self.logger.error(f"Connection health check failed: {str(e)}")
            return False
        return False

    def _consume_wrapper(self):
        """Wrapper method to handle channel start_consuming in a thread"""
        while self.is_running:
            # 1) Ensure we have a live connection+channel
            if not (self.connection and self.connection.is_open and self.channel and self.channel.is_open):
                self.logger.warning("No open connection/channel: connecting...")
                try:
                    # Close any leftovers
                    if self.channel and self.channel.is_open:
                        self.channel.close()
                    if self.connection and self.connection.is_open:
                        self.connection.close()
                except Exception:
                    pass

                if not self.connect():
                    self.logger.error("Failed to connect after retries, sleeping before retry")
                    time.sleep(self.reconnect_delay)
                    continue

                # (Re)declare consumer only once after connect
                try:
                    self.channel.basic_consume(
                        queue=self.queue_name,
                        consumer_tag=self.consumer_tag,
                        on_message_callback=self._handle_callback,
                        auto_ack=False
                    )
                    self.logger.info(f"Consumer registered on queue: {self.queue_name}")
                except Exception as e:
                    self.logger.error(f"Failed to register consumer: {e}")
                    time.sleep(self.reconnect_delay)
                    continue

            # 2) Enter consuming loop
            try:
                self.logger.info(f"Starting consuming on {self.queue_name}")
                self.channel.start_consuming()

            except pika.exceptions.ChannelClosedByBroker as e:
                # e.args[0] is the code, e.args[1] is the text
                code, text = e.args if len(e.args) >= 2 else (None, str(e))
                if code == 406:
                    self.logger.debug(f"Ignoring broker precondition failure: {text}")
                else:
                    self.logger.error(f"Channel closed by broker: {code} {text}")
                # fall through to reconnect

            except pika.exceptions.ConnectionClosed as e:
                self.logger.warning(f"Connection closed: {e}")

            except pika.exceptions.ChannelWrongStateError as e:
                self.logger.warning(f"Channel wrong state: {e}")

            except Exception as e:
                self.logger.error(f"Unexpected consumer error: {e}")

            # 3) Clean up and retry
            try:
                if self.channel and self.channel.is_open:
                    self.channel.close()
                if self.connection and self.connection.is_open:
                    self.connection.close()
            except Exception as e:
                self.logger.error(f"Cleanup after consumer error failed: {e}")

            # Reset and retry
            time.sleep(self.reconnect_delay)
            continue
        
    
    def _handle_callback(self, ch, method, properties, body):
        acked = False  # Track acknowledgment

        try:
            # Ensure channel is open before processing
            if not ch.is_open:
                self.logger.warning("Channel is closed before processing. Reconnecting...")
                self.connect()

            with self.processing_lock:
                if self.error_state:
                    self.logger.warning("Worker in error state, not processing new messages")
                    self._safe_nack(ch, method.delivery_tag, requeue=True)
                    acked = True
                    return

                self.logger.info(f"Processing message from queue: {self.queue_name}")
                start_time = time.time()

                result = self.process_message(ch, method, properties, body)

                processing_time = time.time() - start_time
                self.logger.info(f"Message processed in {processing_time:.2f} seconds")

                if result:
                    if not ch.is_open:
                        self.logger.warning("Channel is closed after processing, skipping ack.")
                        return  # Don't ack if the channel was closed during processing
                    if not acked:
                        self._safe_ack(ch, method.delivery_tag)
                        acked = True
                else:
                    retry_count = properties.headers.get('x-retry-count', 0) if properties.headers else 0
                    if retry_count >= 3:
                        self.logger.warning("Max retries reached, rejecting message")
                        self._safe_reject(ch, method.delivery_tag, requeue=False)
                        acked = True
                    else:
                        self.logger.info(f"Message processing failed, retrying (attempt {retry_count + 1})")
                        self._safe_nack(ch, method.delivery_tag, requeue=True)
                        acked = True  # Ensure the message is marked for requeue

        except Exception as e:
            self.logger.error(f"Error in callback: {str(e)}")
            if not acked:
                try:
                    if ch.is_open:
                        self._safe_nack(ch, method.delivery_tag, requeue=True)
                except Exception as nack_error:
                    self.logger.error(f"Failed to nack during exception: {str(nack_error)}")
            raise  # Re-raise exception for further handling


    def _safe_ack(self, ch, delivery_tag):
        if ch.is_open:
            try:
                ch.basic_ack(delivery_tag=delivery_tag)
                self.logger.info("Message acknowledged")
            except Exception as e:
                self.logger.error(f"Error during ack: {str(e)}")
        else:
            self.logger.warning("Channel already closed, cannot ack message")
    
    def _safe_nack(self, ch, delivery_tag, requeue=True):
        if ch.is_open:
            try:
                ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                self.logger.info(f"Message nacked (requeue={requeue})")
            except Exception as e:
                self.logger.error(f"Error during nack: {str(e)}")
        else:
            self.logger.warning("Channel already closed, cannot nack message")

    def _safe_reject(self, ch, delivery_tag, requeue=False):
        try:
            if ch.is_open:
                ch.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
                self.logger.info("Message rejected (requeue={})".format(requeue))
            else:
                self.logger.warning("Cannot reject: Channel closed")
        except Exception as e:
            self.logger.error(f"Failed to reject message: {str(e)}")

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