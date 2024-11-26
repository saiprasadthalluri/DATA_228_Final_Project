from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import BucketedRandomProjectionLSH

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoLSH") \
    .getOrCreate()

# Input and Output Paths
INPUT_PATH = "s3://mybucketnew1/raw/crypto_data.ndjson"
OUTPUT_PATH = "s3://mybucketnew1/output/crypto_lsh_results/"

# Load the data
data = spark.read.json(INPUT_PATH)

# Select necessary columns and preprocess
lsh_data = data.select(
    col("id").alias("crypto_id"),
    col("market_cap").cast("double"),
    col("total_volume").cast("double"),
    col("price_change_percentage_24h").cast("double"),
    col("circulating_supply").cast("double")
).dropna()

# Feature Engineering: Add derived metrics
lsh_data = lsh_data.withColumn(
    "price_per_unit", col("market_cap") / col("circulating_supply")
).withColumn(
    "liquidity_ratio", col("total_volume") / col("market_cap")
)

# Select features to use for LSH
feature_columns = ["market_cap", "total_volume", "price_change_percentage_24h", "price_per_unit", "liquidity_ratio"]

# Assemble features into a single vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features")
assembled_data = assembler.transform(lsh_data)

# Normalize features using MinMaxScaler
scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
scaler_model = scaler.fit(assembled_data)
normalized_data = scaler_model.transform(assembled_data)

# LSH Model Initialization
lsh = BucketedRandomProjectionLSH(
    inputCol="features",
    outputCol="hashes",
    bucketLength=1e9  # Adjusted bucket length for wider buckets
)
model = lsh.fit(normalized_data)

# Perform Approximate Similarity Join
threshold = 5e12  # Adjust the threshold for neighbor selection
neighbors = model.approxSimilarityJoin(
    normalized_data,
    normalized_data,
    threshold=threshold,
    distCol="EuclideanDistance"
)

# Filter out self-joins and select relevant columns
unique_neighbors = neighbors.filter(
    col("datasetA.crypto_id") != col("datasetB.crypto_id")
).select(
    col("datasetA.crypto_id").alias("crypto_id_A"),
    col("datasetB.crypto_id").alias("crypto_id_B"),
    col("EuclideanDistance")
)

# Save results to S3
unique_neighbors.write.csv(OUTPUT_PATH, header=True, mode="overwrite")

# Stop Spark Session
spark.stop()
