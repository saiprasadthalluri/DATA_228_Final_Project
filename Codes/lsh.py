from pyspark.ml.feature import MinHashLSH
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LSH Example").getOrCreate()

data = [(0, [1.0, 2.0, 3.0]),
        (1, [1.0, 2.0, 3.5]),
        (2, [10.0, 12.0, 14.0])]
df = spark.createDataFrame(data, ["id", "features"])

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=3)
model = mh.fit(df)
model.approxNearestNeighbors(df, [1.0, 2.0, 3.0], 2).show()
