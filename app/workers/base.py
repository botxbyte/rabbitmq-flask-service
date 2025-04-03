from abc import ABC, abstractmethod
import threading
import uuid
from loguru import logger
import os
import json
from app.config.logger import setup_worker_logger

class BaseWorker(ABC):
    def __init__(self, channel, queue_name):
        self.channel = channel
        self.queue_name = queue_name
        self.pid = os.getpid()
        self.logger = setup_worker_logger(self.pid)
        self.consumer_tag = None
        self.thread = None
        self.is_running = True

    def start(self):
        def callback(ch, method, properties, body):
            self.consumer_tag = method.consumer_tag
            if not self.is_running:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            try:
                self.logger.info(f"Received message: {body}")
                data = json.loads(body)
                result = self.process_message(data)

                if result:
                    self.logger.info("Message processed successfully")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.logger.error("Message processing failed")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self.logger.error(f"Error processing message: {str(e)}")
                ch.basic_nack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=False
        )

        self.thread = threading.Thread(
            target=self._consume_wrapper,
            daemon=True
        )
        self.thread.start()
        self.logger.info(f"Worker started with PID: {self.pid}, consumer_tag: {self.consumer_tag}")
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
