import redis
import os

try:
    # Connect to Redis using the Kubernetes service name.
    # It's a good practice to get the host from an environment variable.
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    # r = redis.Redis(host=redis_host, port=6379, db=0)
    r = redis.Redis(host=redis_host, port=31111, db=0)
    
    # Test the connection
    r.ping()
    print("Successfully connected to Redis!")

except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis: {e}")

