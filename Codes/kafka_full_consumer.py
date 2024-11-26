import json
import boto3
from confluent_kafka import Consumer, KafkaException

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'W2456P7LEILCKLFR',
    'sasl.password': 'iXwBlnQ9nZ59wHmkPDU+XlCqpOCivLRmNHlVkzpPyRLSRYUe1nhIdUYMeMuaxEHc',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = "data228-project-topic"
S3_BUCKET_NAME = "mybucketnew1"
S3_FILE_PATH = "raw/crypto_data.ndjson"  # Changed to .ndjson for clarity

# AWS S3 Client
s3_client = boto3.client('s3')

# Consume messages from Kafka and upload to S3
def consume_and_upload_to_s3():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    ndjson_data = ""  # NDJSON formatted string

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            value = msg.value().decode('utf-8')
            # Add each record as a new line in NDJSON format
            ndjson_data += json.dumps(json.loads(value)) + "\n"

        # Upload to S3
        if ndjson_data:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_FILE_PATH,
                Body=ndjson_data.strip(),
                ContentType="application/json"
            )
            print(f"Uploaded records to S3 in NDJSON format: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")
        else:
            print("No records to upload.")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_upload_to_s3()
