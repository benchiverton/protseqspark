from pyspark.sql.types import StructType, StringType

class ProteinSequence:
    def __init__(self, accession: str, geneName: str, specie: str, sequence: str):
        self.accession = accession
        self.geneName = geneName
        self.specie = specie
        self.sequence = sequence
    
    @staticmethod
    def schema() -> StructType:
        return StructType() \
            .add("accession", StringType(), False) \
            .add("geneName", StringType(), False) \
            .add("specie", StringType(), False) \
            .add("sequence", StringType(), False)