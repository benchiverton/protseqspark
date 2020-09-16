class Sequence:
    def __init__(self, accession, geneName, specie, sequence):
        self.accession = accession
        self.geneName = geneName
        self.specie = specie
        self.sequence = sequence
    
    def toTsv(self):
        return  f'{self.accession}\t{self.geneName}\t{self.specie}\t{self.sequence}'