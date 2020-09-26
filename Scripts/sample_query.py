import sys
import os
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: spark-submit --py-files src/dist/*.egg .\src\sample_query.py <parquet_file>

        Assumes you have a parquet file stored in <parquet_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    parquet_file = sys.argv[1]
    
    spark = SparkSession.builder.appName("SampleQuery").getOrCreate()

    sequencesParquetFile = spark.read.parquet(parquet_file)

    filteredSequences = sequencesParquetFile.filter(sequencesParquetFile.sequence.contains("EMIL"))
    filteredSequences.show()

    spark.stop()