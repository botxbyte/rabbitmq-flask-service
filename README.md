# Queue Management API

## Introduction

This Flask-based API manages RabbitMQ queues, allowing users to efficiently scale workers, publish tasks, clear queues, and monitor worker statuses.

## What is RabbitMQ?

RabbitMQ is an open-source message broker that facilitates communication between different parts of a system by sending messages between applications. It implements the Advanced Message Queuing Protocol (AMQP) and allows for reliable messaging, ensuring that messages are delivered even in the event of failures.

### Key Concepts

- **Producer**: An application that sends messages to a queue.
- **Consumer**: An application that receives messages from a queue.
- **Queue**: A buffer that stores messages sent from producers until they are processed by consumers.
- **Exchange**: A routing mechanism that determines how messages are distributed to queues based on routing rules.

## Features

- **Scalable Worker Management**: Dynamically increase or decrease the number of workers for specific queues.
- **Task Publishing**: Publish tasks with worker types and domain names to RabbitMQ queues.
- **Queue Clearing**: Remove all pending messages and stop workers from consuming tasks.
- **Worker Monitoring**: Retrieve active worker details and get workers based on queues or types.

## API Endpoints

### 1. Scale Queue Workers

**Endpoint:** `POST /queue/scale/{queue_name}`

**Description:** Adjust the number of workers for a specific queue.

**Request Body:**

```json
{
  "count": 5,
  "worker_type": "worker1"
}
```

### 2. Publish a Task to a Queue

**Endpoint:** `POST /queue/publish/{queue_name}`

**Description:** Publish a task with a specific worker type and domain name.

**Request Body:**

```json
{
  "worker_type": "scraper",
  "domain_name": "example.com"
}
```

### 3. Clear a Queue

**Endpoint:** `POST /queue/clear/{queue_name}`

**Description:** Stops all workers and removes all messages from the queue.

### 4. Get All Active Workers

**Endpoint:** `GET /queue/workers`

**Description:** Retrieve details of all active workers.

### 5. Get Workers for a Specific Queue

**Endpoint:** `GET /queue/workers/{queue_name}`

**Description:** Retrieve details of workers consuming from a specific queue.

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
- **Consumers (Workers)**: Applications that receive and process messages from queues.

### How RabbitMQ Works in This API:

1. Tasks are **published** to RabbitMQ via the `publish` endpoint.
2. The message is stored in the **queue** until a worker is available to process it.
3. Workers retrieve messages and process them asynchronously.
4. Workers can be **scaled up or down** dynamically based on demand.
5. Queues can be **cleared**, stopping all active workers and removing pending tasks.

### Useful Links:

- [RabbitMQ Official Documentation](https://www.rabbitmq.com/documentation.html)
- [RabbitMQ Management Plugin](https://www.rabbitmq.com/management.html)

## Contribution & Support

For any issues, contributions, or feature requests, open a GitHub issue or contact the maintainer.
