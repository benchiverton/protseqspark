from pyspark.sql.types import StructType, StringType

class sequence:
    def __init__(self, accession, geneName, specie, sequence):
        self.accession = accession
        self.geneName = geneName
        self.specie = specie
        self.sequence = sequence
    
    def toTsv(self):
        return  f'{self.accession}\t{self.geneName}\t{self.specie}\t{self.sequence}'
        
    @staticmethod
    def fromTsv(row):
        parts = row.split("\t"); 
        return sequence(parts[0], parts[1], parts[2], parts[3])
    
    @staticmethod
    def schema():
        return StructType() \
            .add("accession", StringType(), False) \
            .add("geneName", StringType(), False) \
            .add("specie", StringType(), False) \
            .add("sequence", StringType(), False)