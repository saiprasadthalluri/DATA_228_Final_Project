import json
import random
import boto3
import time
from uuid import uuid4
from confluent_kafka import Consumer, KafkaException

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
S3_FILE_PREFIX = "consumer_data/crypto_sample_"
BATCH_LIMIT = 3  # Number of uploads before changing the group ID
SAMPLE_SIZE = 100  # Number of messages to sample per upload

# AWS S3 Client
s3_client = boto3.client('s3')

def create_new_consumer_group():
    """Generate a new unique group ID."""
    return f"crypto-consumer-group-{uuid4()}"

def consume_and_sample_to_s3():
    batch_count = 0
    while True:
        # Create a new consumer group every BATCH_LIMIT uploads
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
                    buffer.append(json.loads(value))

                # Process and upload when buffer has enough messages
                if len(buffer) >= SAMPLE_SIZE:
                    sampled_records = random.sample(buffer, SAMPLE_SIZE)  # Randomly sample messages
                    
                    # Convert to newline-delimited JSON (NDJSON) format
                    ndjson_content = "\n".join(json.dumps(record) for record in sampled_records)
                    
                    s3_file_path = f"{S3_FILE_PREFIX}{random.randint(1, 100000)}.ndjson"
                    
                    # Upload sampled data to S3
                    s3_client.put_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=s3_file_path,
                        Body=ndjson_content,
                        ContentType="application/x-ndjson"
                    )
                    print(f"Uploaded {len(sampled_records)} sampled records to S3: s3://{S3_BUCKET_NAME}/{s3_file_path}")
                    
                    buffer = []  # Clear the buffer after upload
                    batch_count += 1
                    
                    # Change the group ID after BATCH_LIMIT uploads
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
