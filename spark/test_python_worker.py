from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PythonWorkerTest")
    .master("local[1]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Python version seen by Spark:")
print(spark.sparkContext.pythonVer)

data = [(1,), (2,), (3,)]

df = spark.createDataFrame(data, ["number"])

df.show()

spark.stop()