import json
import random
import boto3
import time
from uuid import uuid4
from hashlib import sha256
from confluent_kafka import Consumer, KafkaException
from pybloom_live import BloomFilter
import numpy as np

# Kafka Configuration Template
KAFKA_CONFIG_TEMPLATE = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'W2456P7LEILCKLFR',
    'sasl.password': 'iXwBlnQ9nZ59wHmkPDU+XlCqpOCivLRmNHlVkzpPyRLSRYUe1nhIdUYMeMuaxEHc',
    'auto.offset.reset': 'earliest'
}

TOPIC = "data228-project-topic"
S3_BUCKET_NAME = "mybucketnew1"
S3_FILE_PREFIX = "consumer_data_with_algos/crypto_sample_"
BATCH_LIMIT = 3  # Number of uploads before changing the group ID
SAMPLE_SIZE = 100  # Number of messages to sample per upload
DIFFERENTIAL_PRIVACY = True  # Enable Differential Privacy
RESERVOIR_SAMPLING = True  # Enable Reservoir Sampling
BLOOM_FILTER = True  # Enable Bloom Filter for duplicate detection (log only)
DISTINCT_COUNT = True  # Enable Distinct Count Estimate

# AWS S3 Client
s3_client = boto3.client('s3')

# Bloom Filter
bloom = BloomFilter(capacity=10000, error_rate=0.01)

# Distinct Count Variables
max_zeros = 0

def add_differential_privacy(record, epsilon=1.0):
    """Add Laplace noise to sensitive data fields."""
    if 'market_cap' in record:
        record['market_cap'] += np.random.laplace(0, 1/epsilon)
    return record

def reservoir_sample(data, reservoir, k):
    """Maintain a reservoir sample of size `k`."""
    for i, item in enumerate(data):
        if len(reservoir) < k:
            reservoir.append(item)
        else:
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = item
    return reservoir

def update_distinct_count(record_id):
    """Update distinct count estimation using hashing."""
    global max_zeros
    h = bin(int(sha256(record_id.encode('utf-8')).hexdigest(), 16))[2:]
    trailing_zeros = len(h) - len(h.rstrip('0'))
    max_zeros = max(max_zeros, trailing_zeros)

def create_new_consumer_group():
    """Generate a new unique group ID."""
    return f"crypto-consumer-group-{uuid4()}"

def consume_and_sample_to_s3():
    batch_count = 0
    reservoir = []  # Reservoir for sampling
    while True:
        # Create a new consumer group every `BATCH_LIMIT` uploads
        group_id = create_new_consumer_group()
        kafka_config = KAFKA_CONFIG_TEMPLATE.copy()
        kafka_config['group.id'] = group_id

        print(f"Using new consumer group: {group_id}")
        consumer = Consumer(kafka_config)
        consumer.subscribe([TOPIC])

        buffer = []
        try:
            while True:
                print("Fetching messages from Kafka...")
                
                # Poll messages and buffer them
                for _ in range(1000):  # Poll more messages to ensure consistent buffer
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise KafkaException(msg.error())
                    value = msg.value().decode('utf-8')
                    record = json.loads(value)
                    
                    # Bloom Filter Logging Only
                    if BLOOM_FILTER:
                        if record['id'] in bloom:
                            print(f"Duplicate detected: {record['id']}")
                        bloom.add(record['id'])

                    # Update Distinct Count
                    if DISTINCT_COUNT:
                        update_distinct_count(record['id'])

                    # Add Differential Privacy
                    if DIFFERENTIAL_PRIVACY:
                        record = add_differential_privacy(record)

                    buffer.append(record)

                # Apply Reservoir Sampling
                if RESERVOIR_SAMPLING:
                    reservoir = reservoir_sample(buffer, reservoir, SAMPLE_SIZE)

                # Process and upload when buffer has enough messages
                if len(reservoir) >= SAMPLE_SIZE:
                    s3_file_path = f"{S3_FILE_PREFIX}{random.randint(1, 100000)}.json"
                    
                    # Upload sampled data to S3
                    s3_client.put_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=s3_file_path,
                        Body=json.dumps(reservoir, indent=2),
                        ContentType="application/json"
                    )
                    print(f"Uploaded {len(reservoir)} sampled records to S3: s3://{S3_BUCKET_NAME}/{s3_file_path}")
                    
                    buffer = []  # Clear the buffer after upload
                    reservoir = []  # Reset reservoir
                    batch_count += 1
                    
                    # Print distinct count estimate
                    if DISTINCT_COUNT:
                        distinct_count = 2 ** max_zeros
                        print(f"Estimated Distinct Count: {distinct_count}")

                    # Change the group ID after `BATCH_LIMIT` uploads
                    if batch_count >= BATCH_LIMIT:
                        print("Batch limit reached. Changing consumer group...")
                        break  # Exit the current consumer loop to reset group ID

        except KeyboardInterrupt:
            print("Stopping consumer...")
            break
        finally:
            consumer.close()

if __name__ == "__main__":
    consume_and_sample_to_s3()
