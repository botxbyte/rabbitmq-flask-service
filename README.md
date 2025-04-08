# Queue Management API

## Introduction

This Flask-based API manages RabbitMQ queues, allowing users to efficiently scale workers, publish messages, clear queues, and monitor worker statuses.

## What is RabbitMQ?

RabbitMQ is an open-source message broker that facilitates communication between different parts of a system by sending messages between applications. It implements the Advanced Message Queuing Protocol (AMQP) and allows for reliable messaging, ensuring that messages are delivered even in the event of failures.

### Key Concepts

- **Producer**: An application that sends messages to a queue.
- **Worker**: An application that receives messages from a queue.
- **Queue**: A buffer that stores messages sent from producers until they are processed by workers.
- **Exchange**: A routing mechanism that determines how messages are distributed to queues based on routing rules.
- **Message**: The data sent between producers and workers, which can represent tasks, notifications, or any other information.

## Features

- **Scalable Worker Management**: Dynamically increase or decrease the number of workers for specific queues.
- **Message Publishing**: Publish messages with worker types and domain names to RabbitMQ queues.
- **Queue Clearing**: Remove all pending messages and stop workers from consuming tasks.
- **Worker Monitoring**: Retrieve active worker details and get workers based on queues or types.

## API Endpoints

### 1. Create a Queue

**Endpoint:** `POST /queue/create`

**Description:** Create a new queue.

**Request Body:**

```json
{
  "queue_name": "my_queue"
}
```

### 2. List Queues

**Endpoint:** `GET /queue/list`

**Description:** Retrieve details of all queues.

### 3. Scale Queue Workers

**Endpoint:** `POST /worker/scale/{queue_name}`

**Description:** Adjust the number of workers for a specific queue.

**Request Body:**

```json
{
  "count": 5,
  "worker_name": "worker1"
}
```

### 4. Publish a Message to a Queue

**Endpoint:** `POST /queue/publish/{queue_name}`

**Description:** Publish a message with a specific worker type and domain name.

**Request Body:**

```json
{
  "message": {
    // Pass any message here
  }
}
```

### 5. Clear a Queue

**Endpoint:** `POST /queue/clear/{queue_name}`

**Description:** Stops all workers and removes all messages from the queue.

### 6. Delete a Queue

**Endpoint:** `DELETE /queue/delete/{queue_name}`

**Description:** Delete a specific queue.

### 7. Get Worker Logs

**Endpoint:** `GET /logs/workers/logs/<pid>`

**Description:** Retrieve logs for a specific worker process.

**Response:**

```json
{
  "pid": 1234,
  "log_file": "/path/to/log/file.log",
  "lines": ["Log line 1", "Log line 2", ...]
}
```

### 8. View Worker Logs

**Endpoint:** `GET /workers/view-logs/<pid>`

**Description:** Render the log viewer page for a specific worker process.

**Purpose:** This endpoint allows users to view the logs of a specific worker in a web interface. It is useful for monitoring the worker's activity, debugging issues, and understanding how messages are being processed.

## Creating a New Worker

To create a new worker that processes messages, follow these steps:

1. **Create a new Python file** in the `app/workers/` directory. For example, you can create `worker2.py`.

2. **Define a new worker class** that inherits from `BaseWorker`. Here's a template you can use:

```python
from .base import BaseWorker
from app.config.logger import LoggerSetup
import json

class Worker2(BaseWorker):
    def __init__(self, channel, queue_name):
        super().__init__(channel, queue_name)
        self.logger = self.logger.bind(worker_name="worker2")
        self.worker_name = 'worker2'

    def process_message(self, ch, method, properties, body):
        try:
            data = json.loads(body)
            self.logger.info(f"Processing message: {data}")
            # Your processing logic here
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return True
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return False
```

3. **Implement your message processing logic** in the `process_message` method.

4. **Bind the logger** to your worker class for logging purposes.

5. **Register your new worker** in the `WorkerRoutes` class to make it available for scaling and processing.

## Project Structure

```
REBBITMQ-FLASK-SERVICE/
│── app/
│   ├── config/            # Configuration settings
│   ├── static/            # Static files (if any)
│   ├── __init__.py
│── workers/               # Worker-related logic
│── rabbitmq.py            # RabbitMQ interaction logic
│── routes.py              # API routes for queue operations
│── venv/                  # Virtual environment
│── .env                   # Environment variables
│── .gitignore             # Git ignore file
│── config.py              # Global config settings
│── docker-compose.yml     # Docker configuration
│── Dockerfile             # Docker setup
│── README.md              # Documentation
│── requirements.txt       # Dependencies
```

## Understanding RabbitMQ and Queues

RabbitMQ is an open-source message broker that enables applications to communicate asynchronously. It is widely used for managing background task processing and event-driven architectures.

### Key Concepts:

- **Message Queue**: A queue stores messages until they are consumed by a worker.
- **Exchanges**: Messages are routed to queues based on predefined rules.
- **Bindings**: Links queues to exchanges, defining how messages should be directed.
- **Producers**: Applications that send messages to RabbitMQ.
- **Workers**: Applications that receive and process messages from queues.

### How RabbitMQ Works in This API:

1. Messages are **published** to RabbitMQ via the `publish` endpoint.
2. The message is stored in the **queue** until a worker is available to process it.
3. Workers retrieve messages and process them asynchronously.
4. Workers can be **scaled up or down** dynamically based on demand.
5. Queues can be **cleared**, stopping all active workers and removing pending messages.

### Useful Links:

- [RabbitMQ Official Documentation](https://www.rabbitmq.com/documentation.html)
- [RabbitMQ Management Plugin](https://www.rabbitmq.com/management.html)

## Running the Project

For Production:

```bash
docker-compose down && docker-compose build && docker-compose up -d && docker-comose logs -f
```

For Dev - To run the RabbitMQ Flask service, follow these steps:

1. **Start RabbitMQ Server**: Ensure that RabbitMQ is running on your machine. You can start it using Docker or install it directly on your system.

   If using Docker:

   ```bash
   docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

2. **Activate the Virtual Environment**:

   ```bash
   # On Windows
   venv\Scripts\activate

   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Run the Flask Application**:

   ```bash
   python run.py
   ```

4. **Access the API**: Open your browser and navigate to `http://localhost:5000` to access the API endpoints.

## Contribution & Support

For any issues, contributions, or feature requests, open a GitHub issue or contact the maintainer.
