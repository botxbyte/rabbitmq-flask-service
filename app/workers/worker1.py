from .base import BaseWorker
from app.config.logger import setup_logger
import os
import json

logger = setup_logger()

class Worker1(BaseWorker):
    def __init__(self, channel, queue_name):
        super().__init__(channel, queue_name)
        self.pid = os.getpid()
        self.logger = logger.bind(worker_type="worker1", pid=self.pid)
        self.channel = channel
        self.queue_name = queue_name
        self.worker_type = 'worker1'

    def process_message(self, channel, method, properties, body):
        try:
            if isinstance(body, bytes):
                body = body.decode('utf-8')
            
            message = json.loads(body)
            message_worker_type = message.get("worker_type")
            
            if message_worker_type != self.worker_type:
                self.logger.warning(f"Rejecting message - wrong worker type: {message_worker_type}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                return False
            
            task_id = message.get("task_id")
            data = message.get("data")
            
            self.logger.info(f"Processing task {task_id}")
            
            # Process task logic here
            
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return True
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return False