from confluent_kafka import Consumer
import json
from pyspark.sql import SparkSession

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'VA6DXSX6K225JI2U',  # Replace with your Confluent Kafka API key
    'sasl.password': 'RoRa3tYHOMTFJuj3gUI8V49FNKyk68y/MZSpxXsbHW8oMPQ0RxrhMe54t9MCE8PS',  # Replace with your Confluent Kafka secret
    'group.id': 'cg-consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['coingecko'])

# Spark session for HDFS
spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    record = json.loads(msg.value().decode('utf-8'))
    df = spark.createDataFrame([record])
    df.write.mode('append').json("hdfs:///coin_data/")
