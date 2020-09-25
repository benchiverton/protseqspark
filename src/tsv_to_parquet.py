import sys
import os

from protein_sequences.ProtSeq import ProteinSequence
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: spark-submit --py-files src/dist/*.egg .\src\tsv_to_parquet.py <tsv_file>

        Assumes you have a TSV file stored in <tsv_file>.
        """, file=sys.stderr)
        sys.exit(-1)

    tsv_file = sys.argv[1]
    pre, ext = os.path.splitext(tsv_file)
    parquet_file = f'{pre}.parquet'
    
    spark = SparkSession.builder.appName("TsvToParquet").getOrCreate()

    sequencesDF = spark.read.csv(tsv_file, schema=ProteinSequence.schema(), sep="\t")
    sequencesDF.printSchema()
    sequencesDF.write.parquet(parquet_file)

    spark.stop()