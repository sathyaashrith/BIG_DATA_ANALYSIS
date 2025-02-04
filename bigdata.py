from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import re

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Road Accident Analysis") \
    .getOrCreate()

# Step 2: Load the Dataset
file_path = "road.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 3: Sanitize Column Names
# Define a function to clean column names
def sanitize_column_name(col_name):
    # Remove special characters, spaces, and make it a valid column name
    col_name = re.sub(r"[^a-zA-Z0-9_]", "", col_name)  # Remove special characters
    col_name = re.sub(r"^[^a-zA-Z_]+", "", col_name)  # Remove leading invalid characters
    return col_name

# Rename columns
sanitized_columns = [sanitize_column_name(c) for c in df.columns]
renamed_df = df.toDF(*sanitized_columns)

# Inspect the sanitized column names
print("Sanitized column names:")
renamed_df.printSchema()

# Step 4: Perform Analysis
# Replace "SouthAmerica" and "Dry" with appropriate sanitized column names
average_fatalities = renamed_df.groupBy("SouthAmerica", "Dry") \
    .agg(avg(col("Severe")).alias("Average_Fatalities"))

# Step 5: Display Results
average_fatalities.show()

# Step 6: Save Results
output_path = "/mnt/data/output/average_fatalities_by_region.csv"
average_fatalities.write.csv(output_path, header=True)

# Stop SparkSession
spark.stop()
