import requests
import json
import time

def clear_queue():
    response = requests.post(
        "http://127.0.0.1:5000/queue/clear/urls_queue",
        headers={"Content-Type": "application/json"}
    )
    print(f"Clear queue response: {response.status_code}")
    print(response.json())

def publish_message():
    data = {
        "message": {"data": "hello"},
        "worker_config": {"sequence": ["worker1", "worker2"]}
    }
    response = requests.post(
        "http://127.0.0.1:5000/queue/publish/urls_queue",
        headers={"Content-Type": "application/json"},
        json=data
    )
    print(f"Publish message response: {response.status_code}")
    print(response.json())

if __name__ == "__main__":
    print("Clearing queue...")
    clear_queue()
    time.sleep(2)
    
    print("\nPublishing message...")
    publish_message()
    
    print("\nWaiting for workers to process...")
    time.sleep(5)  # Give workers time to process 