from pyspark.sql.types import StructType, StringType


def proteinSequenceSchema() -> StructType:
    return StructType() \
        .add("accession", StringType(), False) \
        .add("geneName", StringType(), False) \
        .add("specie", StringType(), False) \
        .add("sequence", StringType(), False)
