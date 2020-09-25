import sys
import os

from protein_sequences import sequence
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: sample_query.py --py-files src/dist/*.egg <parquet_file>

        Assumes you have a parquet file stored in <parquet_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    parquet_file = sys.argv[1]
    
    spark = SparkSession.builder.appName("SampleQuery").getOrCreate()

    sequencesParquetFile = spark.read.parquet(parquet_file)

    filteredSequences = sequencesParquetFile.filter(sequencesParquetFile.accession == "Q6F1Z5")
    filteredSequences.show()

    spark.stop()