import pika, os

def get_rabbitmq_connection():
    # Get the current process ID
    pid = os.getpid()
    
    # Set up credentials
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"), 
        os.getenv("RABBITMQ_PASS", "guest")
    )
    
    # Set up connection parameters with client properties
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"), 
        port=int(os.getenv("RABBITMQ_PORT", 5672)), 
        credentials=credentials,
        client_properties={
            'pid': str(pid),
            'connection_name': f'worker-{pid}',
            'app_id': 'rabbitmq-flask-service',  
        }
    )
    
    # Create and return the connection
    return pika.BlockingConnection(parameters)